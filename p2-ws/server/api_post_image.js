
//
// API function: post /image
//
// Returns all images posted by a specific user, 
// or all the images in the database.
//
// Authors:
//	 Jacob Wang
//	 Prof. Joe Hummel
//	 Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name, get_rekognition } = require('./helper.js');
const { ValueError } = require( "./helper.js" );
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

const { PutObjectCommand } = require("@aws-sdk/client-s3");
const { DetectLabelsCommand } = require("@aws-sdk/client-rekognition");
//const { uuid } = require("uuid");
//const { v4: uuidv4 } = require('uuid');

module.exports = { post_image };

/**
* post_image:
*
* @description uploads an image with a unique name to S3, allowing the
* same local file to be uploaded multiple times if desired. A record
* of this image is inserted into the database, and upon success a JSON
* object of the form {message: ..., assetid: ...} is sent where message is
* "success" and assetid is the unique id for this image in the database.
* The image is also analyzed by Rekognition to label
* objects within the image; the results of this analysis are also saved
* in the database (and can be retrieved later via GET /image_labels).
* If an error occurs, a status code of 500 is sent where the JSON object's
* message is the error message and assetid is -1. An invalid userid is
* considered a client-side error, resulting in a status code 400 with
* a message "no such userid" and an assetid of -1.
*
* @param userid (required URL parameter) for whom we are uploading this image
* @param request body {local_filename: string, data: base64-encoded string}
* @returns JSON {message: string, assetid: int}
*/
async function post_image( request, response )
{
	const { v4: uuidv4 } = await import('uuid');

	console.log( "**Call to post /image..." );

	let dbConn = await get_dbConn();

	async function
	lookup_user( userid )
	{
		const lookup_sql = `
			Select username
			From users
			Where userid = ?;
		`;
		try
		{
			const result = await dbConn.execute( lookup_sql, [ userid, ] );
			const [ rows, _colinfo ] = result;

			console.log( `post_image.lookup_user() result:` );
			console.log( result );
			console.log( rows );
			console.log( _colinfo );

			if ( rows.length < 1 )
			{
				throw new ValueError( "no such userid" );
			}
			else if ( rows.length > 1 )
			{
				throw new Error( "unexpected duplicate userid" );
			}
			return rows[0][ 'username' ];
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in post_image.lookup_user():" );
			console.log( err.message );

			throw err;	// re-raise exception to trigger retry mechanism
		}
	}

	async function
	upload_to_bucket( username, local_filename, img_bytes )
	{
		try
		{
			const bkt_name = get_bucket_name();
			const bkt_key = `${ username }/${ uuidv4() }-${ local_filename }`;
			const cmd_params =
				{
					Bucket: bkt_name,
					Key: bkt_key,
					Body: img_bytes
				}
			const cmd = new PutObjectCommand( cmd_params );
			let bkt = get_bucket();
			await bkt.send( cmd );
			return bkt_key;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in post_image.upload_to_bucket():" );
			console.log( err.message );
			throw err;
		}
	}

	async function
	update_db( userid, local_filename, bkt_key )
	{
		const update_sql = `
			Insert Into assets( userid, localname, bucketkey )
			Values( ?, ?, ? );
		`
		await dbConn.beginTransaction();
		try
		{
			const [ info ] = await dbConn.execute(
				update_sql,
				[ userid, local_filename, bkt_key ]
			);
			await dbConn.commit();
			//console.log( "post_images.update_db result:\n" );
			//console.log( info );
			const assetid = info.insertId;
			return assetid;
		}
		catch ( err )
		{
			//
			// exception:
			//
			await dbConn.rollback();
			console.log( "ERROR in post_image.update_db():" );
			console.log( err.message );
			throw err;
		}
	}

	async function
	detect_labels( img_bytes )
	{
		try
		{
			const cmd_params = 
				{
					Image: { Bytes: img_bytes },
					MaxLabels: 10,
					MinConfidence: 90,
				}
			const cmd = new DetectLabelsCommand( cmd_params );
			let rkg = get_rekognition();
			const rkg_labels = await rkg.send( cmd );
			return rkg_labels;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in post_image.detect_labels():" );
			console.log( err.message );
			throw err;
		}
	}

	async function
	update_labels( assetid, rkg_labels )
	{
		const update_label_sql = `
			Insert Into labels( assetid, label, confidence )
			Values( ?, ?, ? );
		`
		await dbConn.beginTransaction();
		try
		{
			for ( let label of rkg_labels.Labels )
			{
				const conf_int = Math.floor( label.Confidence );
				//console.log( `label ${ label.Name } confidence cast from ${ label.Confidence } to ${ conf_int }` );
				await dbConn.execute(
					update_label_sql,
					[ assetid, label.Name, conf_int ]
				);
			}
			await dbConn.commit();
		}
		catch ( err )
		{
			//
			// exception:
			//
			await dbConn.rollback();
			console.log( "ERROR in post_image.update_labels():" );
			console.log( err.message );
			throw err;
		}
	}

	try
	{
		const userid = request.params[ 'userid' ];
		console.log( `post_image userid: ${ userid }` );
		const [ local_filename, img_str ] =
			[ request.body[ 'local_filename' ], request.body[ 'data' ] ];
		const img_bytes = Buffer.from( img_str, 'base64' );

		/* Lookup user */
		const username = await pRetry(
			() => lookup_user( userid ), 
			{ retries: 2 } 
		);

		/* Upload img to bucket */
		const bkt_key = await upload_to_bucket(
			username, local_filename, img_bytes
		);

		/* Register img in db and detect labels concurrently */
		let prom_assetid = pRetry(
			() => update_db( userid, local_filename, bkt_key ), 
			{ retries: 2 } 
		);
		let prom_rkg_labels = detect_labels( img_bytes );
		const [ assetid, rkg_labels ] = await Promise.all(
			[ prom_assetid, prom_rkg_labels ]
		);

		//console.log( "post_image rkg labels" );
		//console.log( rkg_labels );

		/* Register labels in db */
		await pRetry(
			() => update_labels( assetid, rkg_labels ), 
			{ retries: 2 } 
		);

		//const scs_str = `post_image(): success, assetid:${ assetid }`;
		const scs_str = "success";
		console.log( scs_str );
		response.json(
			{
				message: scs_str,
				assetid: assetid,
			}
		);
	}
	catch (err)
	{
		//
		// exception:
		//
		console.log("ERROR:");
		console.log(err.message);
		const stat = err instanceof ValueError ? 400 : 500;
		response.status( stat ).json(
			{
				message: err.message,
				assetid: -1,
			}
		);
	}
	finally
	{
		try
		{
			if ( dbConn ) 
				await dbConn.end();
		}
		finally
		{
		}
	}
	
}

//fvck rekognition

