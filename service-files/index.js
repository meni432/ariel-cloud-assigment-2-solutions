const express = require('express');
const RestaurantsDynamoDBActions = require('./model/restaurantsDynamoDBActions');
const RestaurantsMemcachedActions = require('./model/restaurantsMemcachedActions');
const DataManager = require('./model/dataManager');

const app = express();
app.use(express.json());

const MEMCACHED_CONFIGURATION_ENDPOINT = process.env.MEMCACHED_CONFIGURATION_ENDPOINT;
const TABLE_NAME = process.env.TABLE_NAME;
const AWS_REGION = process.env.AWS_REGION;
const USE_CACHE = process.env.USE_CACHE === 'true';
const dynamoDBActions = new RestaurantsDynamoDBActions(AWS_REGION, TABLE_NAME);
const memcachedActions = new RestaurantsMemcachedActions(MEMCACHED_CONFIGURATION_ENDPOINT);
const dataManager = new DataManager(memcachedActions, dynamoDBActions, USE_CACHE);

app.get('/', (req, res) => {
    const response = {
        MEMCACHED_CONFIGURATION_ENDPOINT: MEMCACHED_CONFIGURATION_ENDPOINT,
        TABLE_NAME: TABLE_NAME,
        AWS_REGION: AWS_REGION
    };
    res.send(response);
});

app.post('/restaurants', async (req, res) => {
    const restaurant = req.body;
    try {
       const result = await dataManager.addRestaurant(restaurant);
       res.send({ success: result });
    } catch (err) {
        if (err.name === 'ConditionalCheckFailedException') {
            res.status(409).send({ success: false , message: 'Restaurant already exists' });
            return;
        }
        console.error(err);
        res.status(500).send('Internal Server Error');
    }
});

app.get('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;
    const restaurant = await dataManager.getRestaurant(restaurantName);
    if (restaurant === false) {
        res.status(404).send('Restaurant not found');
        return;
    }
    res.send(restaurant);
});

app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;
    const result = await dataManager.deleteRestaurant(restaurantName);
    res.send({ success: result });
});

app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const rating = req.body.rating;
    const result = await dataManager.addRestaurantRatingAndCalculateAverage(restaurantName, rating);   
    res.send({ success: result });
});

app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const cuisine = req.params.cuisine;
    let limit = req.query.limit;
    if (limit > 100) {
        res.status(400).send('Limit cannot be greater than 100');
    }
    if (limit === undefined || isNaN(limit)) {
        limit = 10;
    }
    const restaurants = await dataManager.getTopRestaurantsByCuisine(cuisine, limit);
    res.send(restaurants);
});

app.get('/restaurants/region/:region', async (req, res) => {
    const region = req.params.region;
    let limit = req.query.limit;
    if (limit > 100) {
        res.status(400).send('Limit cannot be greater than 100');
    }
    if (limit === undefined || isNaN(limit)) {
        limit = 10;
    }
    const restaurants = await dataManager.getTopRestaurantsByRegion(region, limit);
    res.send(restaurants);
});

app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;
    let limit = req.query.limit;
    if (limit > 100) {
        res.status(400).send('Limit cannot be greater than 100');
    }
    if (limit === undefined || isNaN(limit)) {
        limit = 10;
    }
    const restaurants = await dataManager.getTopRestaurantsByRegionAndCuisine(region, cuisine, limit);
    res.send(restaurants);
});

app.listen(80, () => {
    console.log('Server is running on http://localhost:80');
});

module.exports = { app };