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

module.exports = { get_image_labels };

/**
 * get_image_labels
 *
 * @description when an image is uploaded to S3, Rekognition
 * is automatically called to label objects in the image.
 * Given the image assetid, this function retrieves those labels.
 * If successful the labels are returned as a JSON object of the
 * form {message: ..., data: ...} where message is "success" and
 * data is a list of dictionary-like objects of the form
 * {"label": string, "confidence": int}, ordered by label. If
 * an error occurs, status code of 500 is sent where JSON object's
 * message is the error message and the list is empty []. An
 * invalid assetid is considered a client-side error, resulting
 * in status code 400 with a message "no such assetid" and an empty
 * list [].
 *
 * @param assetid (required URL parameter) of image to retrieve labels for
 * @returns JSON {message: string, data: [object, object, ...]}
 */
async function get_image_labels( request, response )
{
	let dbConn = await get_dbConn();

	async function
	validate_assetid( assetid )
	{
		let lookup_sql = `
			Select assetid
			From assets
			Where assetid = ?;
		`;
		try
		{
			console.log( "executing SQL..." );

			let [ rows, _colinfo ] = await dbConn.execute( lookup_sql, [ assetid, ] );

			if ( rows.length < 1 )
			{
				throw new Error( "no such assetid" );
			}
			else if ( rows.length > 1 )
			{
				throw new Error( "unexpected duplicate assetid" );
			}
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in get_image_labels.validate_assetid():" );
			console.log( err.message );

			throw err;	// re-raise exception to trigger retry mechanism
		}
	}

	async function
	fetch_labels( assetid )
	{
		let lookup_sql = `
			Select label, confidence
			From labels
			Where assetid = ?
			Order By label Asc;
		`;
		try
		{
			console.log( "executing SQL..." );

			const [ rows, _colinfo ] = await dbConn.execute( lookup_sql, [ assetid, ] );

			console.log( `done, retrieved ${rows.length} rows` );

			return rows.map(
				row => (
					{
						label: row[ 'label' ],
						confidence: parseInt( row[ 'confidence' ] )
					}
				)
			);
		}
		catch ( err )
		{
			//
			// exception:
			//
			console.log( "ERROR in get_image_labels.fetch_labels():" );
			console.log( err.message );

			throw err;	// re-raise exception to trigger retry mechanism
		}
	}

	try
	{
		console.log( "**Call to get /image_labels..." );

		const assetid = parseInt( request.params[ "assetid" ] );

		await pRetry(
			() => validate_assetid( assetid ),
			{ retries: 2 }
		);

		//
		// success, return data in JSON format:
		//
		console.log( "success, sending response..." );

		const records = await pRetry(
			() => fetch_labels( assetid ),
			{ retries: 2 }
		);

		const scs_str = "success";
		response.json(
			{
				"message": scs_str,
				"data": records,
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
				"message": err.message,
				"data": [],
			}
		);
	}

};

