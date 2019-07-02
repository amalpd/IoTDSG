package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlin.jvm.internal.Intrinsics
import kotlin.random.Random
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

abstract class SampleBase {
    val logger = LogManager.getLogger()
    val minTravelDistance = 1.0
    val maxTravelDistance = 20.0
    val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
    val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
            Geofence.circle(Location(50.106732, 8.663124), 2.1),
            Geofence.circle(Location(48.877366, 2.359708), 2.1))
    val workloadMachinePerBroker = listOf(3, 3, 3)
    val subsPerBrokerArea = listOf(2, 2, 2)
    val pubsPerBrokerArea = listOf(3, 3, 3)
    val clientsPerBrokerArea = listOf(2, 2, 2)
    var numberOfOverlappingSubscriptionGeofences: Int = 0
    var numberOfOverlappingMessageGeofences: Int = 0
    var numberOfSubscribeMessages: Int = 0
    val timeToRunPerClient = 1800000
    var numberOfPingMessages: Int = 0
    var clientDistanceTravelled: Double = 0.toDouble()
    var numberOfPublishedMessages: Int = 0
    var totalPayloadSize: Int = 0

    fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double): Location {
        var nextLocation: Location

        var relaxFactor = 1.0

        while (true) {
            // calculate travelled distance
            val distance = Random.nextDouble(minTravelDistance, maxTravelDistance)
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

    fun getHeader(): String {
        return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
    }

    fun getSetupString(s: String): String {
        // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
        val c = Class.forName(s)
        val stringBuilder = java.lang.StringBuilder("Setup:\n")

        for (field in c.declaredFields) {
            if (field.name.contains("logger") || field.name.contains("numberOf") || field.name.contains("clientDistanceTravelled") || field.name.contains(
                            "totalPayloadSize")) {

            } else {
                stringBuilder.append("\t").append(field.name).append(": ").append(field.get(c)).append("\n")
            }
        }
        return stringBuilder.toString()

    }

    fun getBrokerTriple(i: Int): Triple<String, Geofence, Int> {
        return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
    }

    fun getBrokerTriple2(i: Int): Triple<String, Geofence, Pair<Int, Int>> {
        return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
    }

    fun checkSubscriptionGeofenceBrokerOverlap(geofence: Geofence) {
        var intersects = -1 // own broker
        brokerAreas.forEach {
            if (geofence.intersects(it)) {
                intersects++
            }
        }
        numberOfOverlappingSubscriptionGeofences += intersects
    }

    fun checkMessageGeofenceBrokerOverlap(geofence: Geofence) {
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
}
