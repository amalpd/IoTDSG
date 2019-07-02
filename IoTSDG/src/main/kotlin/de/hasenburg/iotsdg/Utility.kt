package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlin.random.Random
import org.apache.logging.log4j.LogManager

fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}


fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>,
                    clientsPerBrokerArea: List<Int>): Triple<String, Geofence, Int> {
    return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
}

fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>, subsPerBrokerArea: List<Int>,
                    pubsPerBrokerArea: List<Int>): Triple<String, Geofence, Pair<Int, Int>> {
    return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
}

fun checkSubscriptionGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>): Int {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    return intersects
}

fun checkMessageGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>): Int {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    return intersects
}

fun calculatePingActions(timestamp: Int, location: Location): String {
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"
}

fun getTrueWithChance(chance: Int): Boolean {
    @Suppress("NAME_SHADOWING") var chance = chance
    // normalize
    if (chance > 100) {
        chance = 100
    } else if (chance < 0) {
        chance = 0
    }
    val random = Random.nextInt(100) + 1 // not 0
    return random <= chance
}

fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double,
                          minTravelDistance: Double, maxTravelDistance: Double): Pair<Location, Double> {
    var nextLocation: Location

    var relaxFactor = 1.0

    while (true) {
        // calculate travelled distance
        val distance = Random.nextDouble(minTravelDistance, maxTravelDistance)
        val logger = LogManager.getLogger()
        logger.trace("Travelling for ${distance * 1000}m.")

        // choose a direction (roughly in the direction of the client
        val direction = Random.nextDouble(clientDirection - 10.0, clientDirection + 10.0)

        nextLocation = Location.locationInDistance(location, distance, direction)

        // in case we are at the edge of a geofence, we need to relax it a little bit otherwise this will be an
        // infinite loop
        relaxFactor += 1.0

        if (relaxFactor > 30) {
            // let's go back by 180 degree
            nextLocation = Location.locationInDistance(location, distance, direction + 180.0)
        } else if (relaxFactor > 32) {
            logger.warn("Location $location cannot be used to find another location.")
            return Pair(location, 0.0)
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            return Pair(nextLocation, distance)
        }
    }
}

fun calculateNextLocation(brokerGeofence: Geofence, location: Location, travelTime: Int, clientDirection: Double,
                          minTravelSpeed: Int, maxTravelSpeed: Int): Pair<Location, Double> {
    var nextLocation: Location

    var relaxFactor = 1.0

    while (true) {
        // calculate travelled distance
        val travelSpeed = Random.nextInt(minTravelSpeed, maxTravelSpeed)
        val distance = travelSpeed * (travelTime / 60.0 / 60.0)
        val logger = LogManager.getLogger()
        logger.trace("Travelling with $travelSpeed km/h for $travelTime seconds which leads to ${distance * 1000}m.")

        // choose a direction (roughly in the direction of the client
        val direction = Random.nextDouble(clientDirection - 10.0, clientDirection + 10.0)

        nextLocation = Location.locationInDistance(location, distance, direction)

        // in case we are at the edge of a geofence, we need to relax it a little bit otherwise this will be an
        // infinite loop
        relaxFactor += 1.0

        if (relaxFactor > 30) {
            // let's go back by 180 degree
            nextLocation = Location.locationInDistance(location, distance, direction + 180.0)
        } else if (relaxFactor > 32) {
            logger.warn("Location $location cannot be used to find another location.")
            return Pair(location, 0.0)
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            return Pair(nextLocation, distance)
        }
    }
}