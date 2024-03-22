import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", event);
    const pathParameters = event.pathParameters;
    if (!pathParameters || !pathParameters.awardBody || !pathParameters.movieId) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing path parameters" }),
      };
    }

    const awardBody = pathParameters.awardBody;
    const movieId = parseInt(pathParameters.movieId);

    if(pathParameters && pathParameters.min){
        const min = parseInt(pathParameters.min)
        const commandOutput = await ddbDocClient.send(
            new QueryCommand({
                TableName: process.env.TABLE_NAME,
                KeyConditionExpression: "movieId = :movieId",
                ExpressionAttributeValues: {
                    ":movieId": movieId,
                },
            })
        );

        if (!commandOutput.Items) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "No awards found for the specified movie" }),
            };
        }
        const itemCount = commandOutput.Items.length;
        if (itemCount > min) {
            const body = {
              data1: commandOutput.Items,
            }

            return {
                statusCode: 200,
                headers: {
                  "content-type": "application/json",
                },
                body: JSON.stringify({
                  body,
                }),
              };
        }else{
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Request failed" }),
            };
        }

        
    }
    else{
        const commandOutput = await ddbDocClient.send(
            new QueryCommand({
                TableName: process.env.TABLE_NAME,
                KeyConditionExpression: "movieId = :movieId",
                ExpressionAttributeValues: {
                    ":movieId": movieId,
                },
            })
        );
    
        if (!commandOutput.Items) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "No awards found for the specified movie" }),
            };
        }
        
        const items = commandOutput.Items;
        const filter = items.filter(item => 
            item.reviewerName.includes(awardBody) || item.reviewDate.includes(awardBody)
        );
    
        if (!filter || filter.length === 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "No awards found for the specified movie" }),
            };
        }
    
        const body = {
            data1: filter,
        };
        return {
            statusCode: 200,
            headers: {
              "content-type": "application/json",
            },
            body: JSON.stringify({
              body,
            }),
          };
    }

  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}