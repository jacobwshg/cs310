//
// API function: get /images
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
const { get_dbConn } = require('./helper.js');
//
// p_retry requires the use of a dynamic import:
// const pRetry = require('p-retry');
//
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));


module.exports = { get_images_with_label };

/**
 * get_images_with_label
 *
 * @description when an image is uploaded to S3, Rekognition
 * is automatically called to label objects in the image.
 * These labels are then stored in the database for retrieval / search.
 * Given a label (partial such as 'boat' or complete 'sailboat'), this
 * function performs a case-insensitive search for all images with
 * this label. If successful the labels are returned as a JSON object
 * of the form {message: ..., data: ...} where message is "success" and
 * data is a list of dictionary-like objects of the form {"assetid": int,
 * "label": string, "confidence": int}, ordered by assetid and then label.
 * If an error occurs, status code of 500 is sent where JSON object's
 *  message is the error message and the list is empty.
 *
 * @param label (required URL parameter) to search for, can be a partial word (e.g.
 boat)
 * @returns JSON {message: string, data: [object, object, ...]}
 */

async function
get_images_with_label( request, response )
{
	let dbConn = await get_dbConn();

	async function
	fetch_images( label )
	{
		const lookup_sql = `
			Select assetid, label, confidence
			From labels
			Where label Like ?
			Order By assetid Asc, label Asc;
		`;
		const pattern = `%${ label }%`;
		try
		{
			//
			// call MySQL to execute query, await for results:
			//
			console.log( "executing SQL..." );

			let [ rows, _colinfo ] = await dbConn.execute(
				lookup_sql, [ pattern, ]
			);

			console.log( `done, retrieved ${rows.length} rows` );

			return rows;
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in GIWL.fetch_images():" );
			console.log( err.message );

			throw err;	// re-raise exception to trigger retry mechanism

		}
		finally
		{
			//
			// close connection:
			//
			try
			{
				await dbConn.end();
			} 
			catch ( err ) 
			{ 
				/*ignore*/ 
			}
		}
	}

	//
	// retry the inner function at most 3 times:
	//
	try
	{
		console.log( "**Call to get /images_with_label..." );

		const label = request.params[ 'label' ];

		const rows = await pRetry(
			() => fetch_images( label ),
			{ retries: 2 }
		);

		//
		// success, return data in JSON format:
		//
		console.log( "success, sending response..." );

		console.log( rows );

		const scs_str = "success";
		response.json(
			{
				message: scs_str,
				data: rows,
			}
		);
	}
	catch ( err )
	{
		//
		// exception:
		//
		console.log( "ERROR:" );
		console.log( err.message );

		//
		// if an error occurs it's our fault, so use status code
		// of 500 => server-side error:
		//
		response.status(500).json(
			{
				message: err.message,
				data: [],
			}
		);
	}

};

