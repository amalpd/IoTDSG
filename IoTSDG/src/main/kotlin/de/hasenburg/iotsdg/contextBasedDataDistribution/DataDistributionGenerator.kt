package de.hasenburg.iotsdg.contextBasedDataDistribution

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import org.apache.logging.log4j.LogManager
import kotlin.random.Random


private val logger = LogManager.getLogger()

private const val minTravelDistance = 1.0 //km
private const val maxTravelDistance = 20.0 //km
private const val minPubTimeGap = 2000 //ms
private const val maxPubTimeGap = 150000 //ms
private const val minSubTimeGap = 3000 //ms
private const val maxSubTimeGap = 12000 //ms
private const val minSubRenewalTime = 5 * 60 * 1000 //5 min
private const val maxSubRenewalTime = 60 * 60 * 1000 //60 min

private const val minTemperatureBroadcastMessageGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxTemperatureBroadcastMessageGeofenceDiameter = 10.0 * DistanceUtils.KM_TO_DEG
private const val minHumidityBroadcastMessageGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxHumidityBroadcastMessageGeofenceDiameter = 10.0 * DistanceUtils.KM_TO_DEG
private const val minAnnouncementBroadcastMessageGeofenceDiameter = 40.0 * DistanceUtils.KM_TO_DEG
private const val maxAnnouncementBroadcastMessageGeofenceDiameter = 120.0 * DistanceUtils.KM_TO_DEG

private const val temperaturePublicationProbability = 100
private const val humidityPublicationProbability = 100
private const val announcementPublicationProbability = 100
private const val mobilityProbability = 40
private const val temperaturePayloadSize = 100
private const val minHumidityPayloadSize = 50
private const val maxHumidityPayloadSize = 150
private const val minAnnouncementPayloadSize = 10
private const val maxAnnouncementPayloadSize = 75

private const val directoryPath = "./dataDistribution"
private const val temperatureTopic = "temperature"
private const val humidityTopic = "humidity"
private const val announcementTopic = "public_announcement"

private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val workloadMachinePerBroker = listOf(3, 3, 3)
private val subsPerBrokerArea = listOf(2, 2, 2)
private val pubsPerBrokerArea = listOf(3, 3, 3)

private var numberOfSubscribeMessages = 0
private var timeToRunPerClient = 1800000
private var numberOfPingMessages = 0
private var clientDistanceTravelled = 0.0
private var numberOfPublishedMessages = 0
private var totalPayloadSize = 0
private var numberOfOverlappingMessageGeofences = 0

fun main() {

    for (ba in brokerAreas) {
        var numberOfOverlaps = 0
        for (baI in brokerAreas) {
            if (ba.intersects(baI)) {
                numberOfOverlaps++
            }
        }

        if (numberOfOverlaps > 1) {
            logger.fatal("Brokers should not overlap!")
            System.exit(1)
        }
    }

    // make sure dir exists, delete old content
    val dir = File(directoryPath)
    if (dir.exists()) {
        logger.info("Deleting old content")
        dir.deleteRecursively()
    }
    dir.mkdirs()

    val setup = getSetupString()
    logger.info(setup)
    File("$directoryPath/02_summary.txt").writeText(setup)

    for (b in 0..2) {

        val broker = getBrokerTriple(b)
        var currentWorkloadMachine: Int

        for (pub in 0..broker.third.second) {

            if (workloadMachinePerBroker.get(b) == 0) {
                logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                break
            }
            currentWorkloadMachine = pub % workloadMachinePerBroker.get(b)
            val clientName = randomName()
            val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Pub-$clientName.csv")
            var timestamp = Random.nextInt(0, 2000)
            val writer = file.bufferedWriter()
            var location = Location.randomInGeofence(broker.second)
            writer.write(getHeader())

            /*writer.write(calculatePingActions(timestamp, location))
            * not important */

            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePublishActions(timestamp, location))
                if(getTrueWithChance(mobilityProbability)){
                    location = calculateNextLocation(broker.second, location, Random.nextDouble(0.0, 360.0))
                }
                timestamp += Random.nextInt(minPubTimeGap, maxPubTimeGap)
            }
            writer.flush()
            writer.close()
        }

        for (c in 0..broker.third.first) {

            if (workloadMachinePerBroker.get(b) == 0) {
                logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                break
            }
            currentWorkloadMachine = c % workloadMachinePerBroker.get(b)
            val clientName = randomName()
            logger.debug("Calculating actions for client $clientName")
            var location = Location.randomInGeofence(broker.second)
            var timestamp = Random.nextInt(0, 3000)
            val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Sub-$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())
            writer.write(calculateSubscribeActions(timestamp, location))
            timestamp += 1000
            var subRenewalTime = Random.nextInt(minSubRenewalTime, maxSubRenewalTime)

            while (timestamp <= timeToRunPerClient) {

                if (timestamp >= subRenewalTime) {
                    writer.write(calculateSubscribeActions(timestamp, location))
                    subRenewalTime += Random.nextInt(minSubRenewalTime, maxSubRenewalTime)
                }
                location = calculateNextLocation(broker.second, location, Random.nextDouble(0.0, 360.0))
                writer.write(calculatePingActions(timestamp, location))
                timestamp += Random.nextInt(minSubTimeGap, maxSubTimeGap)
            }
            writer.flush()
            writer.close()
        }


    }
    val distancePerClient = clientDistanceTravelled / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)
    val output = """Data set characteristics:
    Number of ping messages: $numberOfPingMessages (${numberOfPingMessages / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_subscriber)
    Number of subscribe messages: $numberOfSubscribeMessages (${numberOfSubscribeMessages / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_subscriber)
    Number of publish messages: $numberOfPublishedMessages (${numberOfPublishedMessages / (pubsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_publisher)
    Publish payload size: ${totalPayloadSize / 1000.0} KB (${totalPayloadSize / numberOfPublishedMessages} byte/message)
    Client distance travelled: ${clientDistanceTravelled}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / timeToRunPerClient * 3600} km/h
    Number of subscription geofence broker overlaps: $numberOfOverlappingMessageGeofences"""

    logger.info(output)
    File("$directoryPath/02_summary.txt").appendText(output)
}

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

fun getBrokerTriple(i: Int): Triple<String, Geofence, Pair<Int, Int>> {
    return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
}

fun calculateSubscribeActions(timestamp: Int, location: Location): String {

    val actions = StringBuilder()

    // temperature
    actions.append("${timestamp + 1};${location.lat};${location.lon};subscribe;" + "$temperatureTopic;;\n")
    numberOfSubscribeMessages++

    // humidity
    actions.append("${timestamp + 2};${location.lat};${location.lon};subscribe;" + "$humidityTopic;;\n")
    numberOfSubscribeMessages++

    // barometric pressure
    actions.append("${timestamp + 3};${location.lat};${location.lon};subscribe;" + "$announcementTopic;;\n")
    numberOfSubscribeMessages++

    return actions.toString()
}

fun getSetupString(): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName("de.hasenburg.iotsdg.contextBasedDataDistribution.DataDistributionGeneratorKt")
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

fun calculatePingActions(timestamp: Int, location: Location): String {
    numberOfPingMessages++
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"
}

fun calculatePublishActions(timestamp: Int, location: Location): String {
    val actions = StringBuilder()

    // temperature condition
    if (getTrueWithChance(temperaturePublicationProbability)) {
        val geofenceTB = Geofence.circle(location,
                Random.nextDouble(minTemperatureBroadcastMessageGeofenceDiameter,
                        maxTemperatureBroadcastMessageGeofenceDiameter))
        checkMessageGeofenceBrokerOverlap(geofenceTB)
        actions.append("${timestamp + 4};${location.lat};${location.lon};publish;" + "$temperatureTopic;" + "${geofenceTB.wktString};" + "$temperaturePayloadSize\n")
        numberOfPublishedMessages++
        totalPayloadSize += temperaturePayloadSize
    }

    // humidity broadcast
    if (getTrueWithChance(humidityPublicationProbability)) {
        val geofenceH = Geofence.circle(location,
                Random.nextDouble(minHumidityBroadcastMessageGeofenceDiameter,
                        maxHumidityBroadcastMessageGeofenceDiameter))
        checkMessageGeofenceBrokerOverlap(geofenceH)
        val payloadSize = Random.nextInt(minHumidityPayloadSize, maxHumidityPayloadSize)
        actions.append("${timestamp + 5};${location.lat};${location.lon};publish;" + "$humidityTopic;" + "${geofenceH.wktString};" + "$payloadSize\n")
        totalPayloadSize += payloadSize
        numberOfPublishedMessages++
    }

    // public announcement broadcast
    if (getTrueWithChance(announcementPublicationProbability)) {
        val geofencePA = Geofence.circle(location,
                Random.nextDouble(minAnnouncementBroadcastMessageGeofenceDiameter,
                        maxAnnouncementBroadcastMessageGeofenceDiameter))
        checkMessageGeofenceBrokerOverlap(geofencePA)
        val payloadSize = Random.nextInt(minAnnouncementPayloadSize, maxAnnouncementPayloadSize)
        actions.append("${timestamp + 6};${location.lat};${location.lon};publish;" + "$announcementTopic;" + "${geofencePA.wktString};" + "$payloadSize\n")
        totalPayloadSize += payloadSize
        numberOfPublishedMessages++
    }
    return actions.toString()
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