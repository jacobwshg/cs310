
//
// API function: delete /images
//
// Clear all images and their records from bucket and database.
//
// Authors:
//	 Jacob Wang
//	 Prof. Joe Hummel
//	 Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name, get_rekognition } = require('./helper.js');
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

const { DeleteObjectsCommand } = require("@aws-sdk/client-s3");

module.exports = { delete_images };

/**
 * delete_images:
 *
 * @description deletes all images and associated labels from the
 * database and S3. If successful, returns a JSON object of the
 * form {message: string} where the message is "success". If an
 * error occurs, message will carry the error message. The images
 * are not deleted from S3 unless the database is successfully
 * cleared; if an error occurs either (a) there are no changes or
 * (b) the database is cleared but there may be one or more images
 * remaining in S3 (which has no negative effect since they have
 * unique names).
 *
 * @param none
 * @returns JSON {message: string}
 */
async function
delete_images( request, response )
{
	console.log( "**Call to delete /images..." );

	let dbConn = await get_dbConn();

	async function
	get_keys()
	{
		const lookup_sql = `
			Select bucketkey
			From assets;
		`;
		try
		{
			const result = await dbConn.query( lookup_sql, [] );
			const [ rows, _colinfo ] = result;

			const keys = rows.map(
				row => row[ 'bucketkey' ]
			);

			return keys;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in delete_images.get_keys():" );
			console.log( err.message );

			throw err;
		}
	}

	async function
	clear_db()
	{
		const delete_sql = 
			[
				`SET foreign_key_checks = 0;`,
				`TRUNCATE TABLE labels;`,
				`TRUNCATE TABLE assets;`,
				`SET foreign_key_checks = 1;`,
				`ALTER TABLE assets AUTO_INCREMENT = 1001;`,
			];

		await dbConn.beginTransaction();

		try
		{
			for ( clause of delete_sql )
			{
				await dbConn.execute( clause );
			}
			await dbConn.commit();
		}
		catch ( err )
		{
			//
			// exception:
			//
			await dbConn.rollback();
			console.log( "ERROR in delete_images.clear_db():" );
			console.log( err.message );
			throw err;
		}
	}

	async function
	clear_bkt( keydict )
	{
		try
		{
			const bkt_name = get_bucket_name();
			const cmd_params =
				{
					Bucket: bkt_name,
					Delete: { Objects: keydict },
				};
			const cmd = new DeleteObjectsCommand( cmd_params );
			let bkt = get_bucket();
			await bkt.send( cmd );
		}

		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in delete_images.clear_bkt():" );
			console.log( err.message );
			throw err;
		}
	}

	try
	{
		/* Validate assetid */
		const keys = await pRetry(
			() => get_keys(),
			{ retries: 2 }
		);

		console.log( "delete_images keys:" );
		console.log( keys );

		const keydict = keys.map(
			// key => { Key: key }  // parsed as fn body
			key => ( { Key: key } )
		);
		console.log( "delete_images keydict" );
		console.log( keydict );

		//while ( `a` ) {}

		await clear_db();
		if ( keydict.length > 0 )
		{
			await clear_bkt( keydict );
		}

		const scs_str = "success"; 
		console.log( scs_str );
		response.json(
			{
				message: scs_str,
			}
		);
	}
	catch (err)
	{
		//
		// exception:
		//
		console.log("ERROR in delete_images():");
		console.log(err.message);

		response.status(500).json(
			{
				message: err.message,
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

