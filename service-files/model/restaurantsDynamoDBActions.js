// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/dynamodb/

const { DynamoDBClient, PutItemCommand, UpdateItemCommand, GetItemCommand, QueryCommand, DeleteItemCommand } = require("@aws-sdk/client-dynamodb");

// PK: RestaurantName (String)
// GSI: Cuisine (String), Rating (Number)
// GSI: GeoRegion (String), Rating (Number)
// GSI GeoRegion_Cuisine (String), Rating (Number)
// Column for raiting, Raiting_Count, Total_Rate, Rating (Number, avg for all ratings)

class RestaurantsDynamoDBActions {
    /**
     * 
     * @param {string} region 
     * @param {string} tableName 
     */
    constructor(region, tableName) {
        this.client = new DynamoDBClient({ region: region });
        this.tableName = tableName;
    }

    async addRestaurant(restaurant) {
        const params = {
            TableName: this.tableName,
            Item: {
                "RestaurantName": { S: restaurant.name },
                "Cuisine": { S: restaurant.cuisine },
                "Rating": { N: restaurant.rating.toString() },
                "Rating_Count": { N: "0" },
                "Total_Rate": { N: "0" },
                "GeoRegion": { S: restaurant.region },
                "GeoRegion_Cuisine": { S: restaurant.region + "_" + restaurant.cuisine }
            },
            ConditionExpression: "attribute_not_exists(RestaurantName)"
        };

        try {
            const data = await this.client.send(new PutItemCommand(params));
            console.log("Success", data);
            return true;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    async deleteRestaurant(restaurantName) {
        const params = {
            TableName: this.tableName,
            Key: {
                "RestaurantName": { S: restaurantName }
            }
        };

        try {
            const data = await this.client.send(new DeleteItemCommand(params));
            console.log("Success", data);
            return true;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    async getRestaurant(restaurantName) {
        const params = {
            TableName: this.tableName,
            Key: {
                "RestaurantName": { S: restaurantName }
            }
        };

        try {
            const data = await this.client.send(new GetItemCommand(params));
            // map the data to json object
            const item = data.Item;
            if (item === undefined) {
                return false;
            }
            const restaurant = this.__convertDynamoEntityToRestaurant(item);
            console.log("Success", data);
            return restaurant;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    async addRestaurantRatingAndCalculateAverage(restaurantName, rating) {
        const params = {
            TableName: this.tableName,
            Key: {
                "RestaurantName": { S: restaurantName }
            },
            UpdateExpression: "ADD Rating_Count :count, Total_Rate :total",
            ExpressionAttributeValues: {
                ":count": { N: "1" },
                ":total": { N: rating.toString() }
            },
            ReturnValues: "ALL_NEW"
        };

        try {
            const data = await this.client.send(new UpdateItemCommand(params));
            // calculate the average rating
            const rating = Number(data.Attributes.Total_Rate.N) / Number(data.Attributes.Rating_Count.N);
            // update the rating
            const updateParams = {
                TableName: this.tableName,
                Key: {
                    "RestaurantName": { S: restaurantName }
                },
                UpdateExpression: "SET Rating = :rating",
                ExpressionAttributeValues: {
                    ":rating": { N: rating.toString() }
                }
            };
            await this.client.send(new UpdateItemCommand(updateParams));
            return true;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    async getTopRestaurantsByCuisine(cuisine, limit) {
        const params = {
            TableName: this.tableName,
            IndexName: "Cuisine-Rating-index",
            KeyConditionExpression: "Cuisine = :cuisine",
            ExpressionAttributeValues: {
                ":cuisine": { S: cuisine }
            },
            Limit: limit,
            ScanIndexForward: false
        };

        try {
            const data = await this.client.send(new QueryCommand(params));
            const restaurants = data.Items.map(item => this.__convertDynamoEntityToRestaurant(item));
            console.log("Success", data);
            return restaurants;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    async getTopRestaurantsByRegion(region, limit) {
        const params = {
            TableName: this.tableName,
            IndexName: "GeoRegion-Rating-index",
            KeyConditionExpression: "GeoRegion = :geoRegion",
            ExpressionAttributeValues: {
                ":geoRegion": { S: region }
            },
            Limit: limit,
            ScanIndexForward: false
        };

        try {
            const data = await this.client.send(new QueryCommand(params));
            const restaurants = data.Items.map(item => this.__convertDynamoEntityToRestaurant(item));
            console.log("Success", data);
            return restaurants;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }
    
    async getTopRestaurantsByRegionAndCuisine(region, cuisine, limit) {
        const params = {
            TableName: this.tableName,
            IndexName: "GeoRegion_Cuisine-Rating-index",
            KeyConditionExpression: "GeoRegion_Cuisine = :region_cuisine",
            ExpressionAttributeValues: {
                ":region_cuisine": { S: region + "_" + cuisine }
            },
            Limit: limit,
            ScanIndexForward: false
        };

        try {
            const data = await this.client.send(new QueryCommand(params));
            const restaurants = data.Items.map(item => this.__convertDynamoEntityToRestaurant(item));
            console.log("Success", data);
            return restaurants;
        } catch (err) {
            console.log("Error", err);
            throw err;
        }
    }

    __convertDynamoEntityToRestaurant(entity) {
        return {
            name: entity.RestaurantName.S,
            cuisine: entity.Cuisine.S,
            rating: Number(entity.Rating.N),
            region: entity.GeoRegion.S
        };
    }
}

module.exports = RestaurantsDynamoDBActions;
