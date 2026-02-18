
//
// API function: post /image
//
// Downloads an image identified by its assetid. 
//
// Authors:
//	 Jacob Wang
//	 Prof. Joe Hummel
//	 Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name, get_rekognition } = require('./helper.js');
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

const { GetObjectCommand } = require("@aws-sdk/client-s3");

module.exports = { get_image };

/**
 * get_image:
 *
 * @description downloads an image denoted by the given asset id. If
 * successful a JSON object of the form {message: string, userid: int,
 * local_filename: string, data: base64-encoded string} is sent where
 * message is "success" and the remaining values are set based on the
 * image downloaded; data is image contents as base64-encoded string.
 * If an error occurs, a status code of 500 is sent where JSON object's
 * message is the error message and userid is -1; the other values are
 * undefined. An invalid assetid is considered a client-side error,
 * resulting in a status code 400 with a message "no such assetid"
 * and a userid of -1; the other values are undefined.
 *
 * @param assetid (required URL parameter) of image to download
 * @returns JSON {message: string, userid: int, local_filename: string,
 data: base64-encoded string}
*/
async function
get_image( request, response )
{
	console.log( "**Call to get /image..." );

	let dbConn = await get_dbConn();

	async function
	validate_assetid( assetid )
	{
		const lookup_sql = `
			Select userid, localname, bucketkey
			From assets
			Where assetid = ?;
		`;
		try
		{
			const result = await dbConn.execute( lookup_sql, [ assetid, ] );
			console.log( `get_image validate_assetid result:` );
			console.log( result )
			const [ rows, _colinfo ] = result;

			if ( rows.length < 1 )
			{
				throw new Error( "get_image: no such assetid" );
			}
			else if ( rows.length > 1 )
			{
				throw new Error( "get_image: unexpected duplicate assetid" );
			}
			const row = rows[0];
			return row;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in get_image.validate_assetid():" );
			console.log( err.message );

			throw err; // re-raise exception to trigger retry mechanism
		}
	}

	async function
	download_image_str( bkt_key )
	{
		try
		{
			const bkt_name = get_bucket_name();
			const cmd_params =
				{
					Bucket: bkt_name,
					Key: bkt_key,
				}
			const cmd = new GetObjectCommand( cmd_params );
			let bkt = get_bucket();
			const img_data = await bkt.send( cmd );
			const img_bytes = img_data.Body;
			const img_str = await img_bytes.transformToString( 'base64' );

			return img_str;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in get_image.download_image_str():" );
			console.log( err.message );
			throw err;
		}
	}

	try
	{
		const assetid = parseInt( request.params[ 'assetid' ] );
		if ( isNaN( assetid ) )
		{
			throw new Error( `get_image: assetid is not numeric` );
		}
		console.log( `get_image assetid: ${ assetid }` );

		/* Validate assetid */
		const row = await pRetry(
			() => validate_assetid( assetid ),
			{ retries: 2 }
		);
		console.log("\n\n*********row:");
		console.log( row );
		// avoid `(intermediate value) is not iterable`
		const [ userid, db_localname, bkt_key ] =
			[ row[ 'userid' ], row[ 'localname' ], row[ 'bucketkey' ] ];

		/* Retrive image from bucket and encode as string */
		const img_str = await download_image_str( bkt_key );

		let local_filename = db_localname;

		const scs_str = "success"; 
		console.log( scs_str );
		response.json(
			{
				message:        scs_str,
				userid:         userid,
				local_filename: local_filename,
				data:           img_str,
			}
		);
	}
	catch (err)
	{
		//
		// exception:
		//
		console.log("ERROR in get_image():");
		console.log(err.message);

		response.status(500).json(
			{
				message: err.message,
				userid:  -1,
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

