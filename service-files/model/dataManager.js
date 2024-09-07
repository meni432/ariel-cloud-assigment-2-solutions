
const RestaurantsDynamoDBActions = require('./restaurantsDynamoDBActions');
const RestaurantsMemcachedActions = require('./restaurantsMemcachedActions');

class DataManager {
    /**
     * 
     * @param {RestaurantsMemcachedActions} restaurantsMemcachedActions 
     * @param {RestaurantsDynamoDBActions} restaurantsDynamoDBActions 
     * @param {boolean} useCache 
     */
    constructor(restaurantsMemcachedActions, restaurantsDynamoDBActions, useCache) {
        this.restaurantsMemcachedActions = restaurantsMemcachedActions;
        this.restaurantsDynamoDBActions = restaurantsDynamoDBActions;
        this.useCache = useCache;
    }

    async addRestaurant(restaurant) {
        const extendedRestaurant = {
            ...restaurant,
            rating: 0
        };

        const dbResult = await this.restaurantsDynamoDBActions.addRestaurant(extendedRestaurant);
        if (this.useCache && dbResult) {
            await this.__storeRestaurantInCache(extendedRestaurant);
        }
        return true;
    }

    async deleteRestaurant(restaurantName) {
        const dbResult = await this.restaurantsDynamoDBActions.deleteRestaurant(restaurantName);
        if (this.useCache && dbResult) {
            await this.__deleteRestaurantFromCache(restaurantName);
        }
        return dbResult;
    }

    async getRestaurant(restaurantName) {
        if (!this.useCache) {
            return await this.restaurantsDynamoDBActions.getRestaurant(restaurantName);
        }

        let restaurant = await this.__getRestaurantFromCache(restaurantName);
        console.log('MS :: restaurant', restaurant);
        if (!restaurant) {
            restaurant = await this.restaurantsDynamoDBActions.getRestaurant(restaurantName);
            if (restaurant) {
                await this.__storeRestaurantInCache(restaurant);
            }
        }
        return restaurant;
    }

    async addRestaurantRatingAndCalculateAverage(restaurantName, rating) {
        const result = await this.restaurantsDynamoDBActions.addRestaurantRatingAndCalculateAverage(restaurantName, rating);
        const updatedRestaurant = await this.restaurantsDynamoDBActions.getRestaurant(restaurantName);
        if (this.useCache && updatedRestaurant !== null) {
            await this.__storeRestaurantInCache(updatedRestaurant);
        }
        return result;
    }

    async getTopRestaurantsByCuisine(cuisine, limit) {
        const cacheKey = `${cuisine}_${limit}`;
        if (this.useCache) {
            const restaurants = await this.restaurantsMemcachedActions.getRestaurants(cacheKey);
            if (restaurants) {
                return restaurants;
            }
        }
        const dbResult = await this.restaurantsDynamoDBActions.getTopRestaurantsByCuisine(cuisine, limit);
        if (this.useCache && dbResult) {
            await this.restaurantsMemcachedActions.addRestaurants(cacheKey, dbResult);
        }
        return dbResult;
    }

    async getTopRestaurantsByRegion(region, limit) {
        const cacheKey = `${region}_${limit}`;
        if (this.useCache) {
            const restaurants = await this.restaurantsMemcachedActions.getRestaurants(cacheKey);
            if (restaurants) {
                return restaurants;
            }
        }
        const dbResult = await this.restaurantsDynamoDBActions.getTopRestaurantsByRegion(region, limit);
        if (this.useCache && dbResult) {
            await this.restaurantsMemcachedActions.addRestaurants(cacheKey, dbResult);
        }
        return dbResult;
    }

    async getTopRestaurantsByRegionAndCuisine(region, cuisine, limit) {
        const cacheKey = `${region}_${cuisine}_${limit}`;
        if (this.useCache) {
            const restaurants = await this.restaurantsMemcachedActions.getRestaurants(cacheKey);
            if (restaurants) {
                return restaurants;
            }
        }
        const dbResult = await this.restaurantsDynamoDBActions.getTopRestaurantsByRegionAndCuisine(region, cuisine, limit);
        if (this.useCache && dbResult) {
            await this.restaurantsMemcachedActions.addRestaurants(cacheKey, dbResult);
        }
        return dbResult;
    }

    async __storeRestaurantInCache(restaurant) {
        const cacheKey = `r-${restaurant.name}`;
        return await this.restaurantsMemcachedActions.addRestaurants(cacheKey, restaurant);
    }

    async __deleteRestaurantFromCache(restaurantName) {
        const cacheKey = `r-${restaurantName}`;
        return await this.restaurantsMemcachedActions.deleteRestaurants(cacheKey);
    }

    async __getRestaurantFromCache(restaurantName) {
        const cacheKey = `r-${restaurantName}`;
        return await this.restaurantsMemcachedActions.getRestaurants(cacheKey);
    }
}

module.exports = DataManager;