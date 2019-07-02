package de.hasenburg.iotsdg.utility

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlin.random.Random
import org.apache.logging.log4j.LogManager

// Stats
private var numberOfPingMessages = 0
private var clientDistanceTravelled = 0.0
private var numberOfPublishedMessages = 0
private var totalPayloadSize = 0
private var numberOfOverlappingSubscriptionGeofences = 0
private var numberOfSubscribeMessages = 0
private var numberOfOverlappingMessageGeofences = 0

//returns the title row
fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}

// returns a triple of brokername, broker area and clients in the area
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>,
                    clientsPerBrokerArea: List<Int>): Triple<String, Geofence, Int> {
    return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
}

//returns a triple of brokernam, broker area, subscribers and publishers in the area
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>, subsPerBrokerArea: List<Int>,
                    pubsPerBrokerArea: List<Int>): Triple<String, Geofence, Pair<Int, Int>> {
    return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
}

// count no. of subscription overlaps
fun checkSubscriptionGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    numberOfOverlappingSubscriptionGeofences += intersects
}

// count no. of message overlaps
fun checkMessageGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    numberOfOverlappingMessageGeofences += intersects
}

fun calculatePingActions(timestamp: Int, location: Location): String {
    numberOfPingMessages++
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"
}

/**
 * Returns true with the given chance.
 *
 * @param chance - the chance to return true (0 - 100)
 * @return true, if lucky
 */
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

/**
 * Calculate a new location based upon
 * TravelDistance, previous location and Direction
 * New Location should also be within the same broker area
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double,
                          minTravelDistance: Double, maxTravelDistance: Double): Location {
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
            return location
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            clientDistanceTravelled += distance
            return nextLocation
        }
    }
}

/**
 * Calculates new Location based upon
 * TravelSpeed, previous location and Direction
 * New Location should be within the same broker area
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, travelTime: Int, clientDirection: Double,
                          minTravelSpeed: Int, maxTravelSpeed: Int): Location {
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
            return location
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            clientDistanceTravelled += distance
            return nextLocation
        }
    }
}

// checking overlapping brokerareas
fun checkBrokerOverlap(brokerAreas: List<Geofence>): Boolean {
    for (ba in brokerAreas) {
        var numberOfOverlaps = 0
        for (baI in brokerAreas) {
            if (ba.intersects(baI)) {
                numberOfOverlaps++
            }
        }

        if (numberOfOverlaps > 1) {
            return true
        }
    }
    return false
}


/**
 * Updating Stats
 */
fun addSubscribeMessages() {
    numberOfSubscribeMessages++
}

fun addPublishMessages() {
    numberOfPublishedMessages++
}

fun addPayloadSize(size: Int) {
    totalPayloadSize += size
}

/**
 * getting Stats*/
fun getnumberOfPingMessages(): Int {
    return numberOfPingMessages
}

fun getnumberOfPublishedMessages(): Int {
    return numberOfPublishedMessages
}

fun getnumberOfSubscribeMessages(): Int {
    return numberOfSubscribeMessages
}

fun getclientDistanceTravelled(): Double {
    return clientDistanceTravelled
}

fun getnumberOfOverlappingSubscriptionGeofences(): Int {
    return numberOfOverlappingSubscriptionGeofences
}

fun getnumberOfOverlappingMessageGeofences(): Int {
    return numberOfOverlappingMessageGeofences
}

fun gettotalPayloadSize(): Int {
    return totalPayloadSize
}
