package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import kotlin.random.Random

class Sample : SampleBase(){

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
}

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

    private const val directoryPath = "./Sample"
    private const val temperatureTopic = "temperature"
    private const val humidityTopic = "humidity"
    private const val announcementTopic = "public_announcement"

    fun main() {
        val a = Sample()
        for (ba in a.brokerAreas) {
            var numberOfOverlaps = 0
            for (baI in a.brokerAreas) {
                if (ba.intersects(baI)) {
                    numberOfOverlaps++
                }
            }

            if (numberOfOverlaps > 1) {
                a.logger.fatal("Brokers should not overlap!")
                System.exit(1)
            }
        }

        // make sure dir exists, delete old content
        val dir = File(directoryPath)
        if (dir.exists()) {
            a.logger.info("Deleting old content")
            dir.deleteRecursively()
        }
        dir.mkdirs()

        val setup = a.getSetupString("de.hasenburg.iotsdg.SampleKt")
        a.logger.info(setup)
        File("$directoryPath/02_summary.txt").writeText(setup)

        for (b in 0..2) {

            val broker = a.getBrokerTriple2(b)
            var currentWorkloadMachine: Int

            for (pub in 0..broker.third.second) {

                if (a.workloadMachinePerBroker.get(b) == 0) {
                    a.logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                    break
                }
                currentWorkloadMachine = pub % a.workloadMachinePerBroker.get(b)
                val clientName = randomName()
                val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Pub-$clientName.csv")
                var timestamp = Random.nextInt(0, 2000)
                val writer = file.bufferedWriter()
                var location = Location.randomInGeofence(broker.second)
                writer.write(getHeader())

                /*writer.write(calculatePingActions(timestamp, location))
                * not important */

                while (timestamp <= a.timeToRunPerClient) {
                    writer.write(a.calculatePublishActions(timestamp, location))
                    if(getTrueWithChance(mobilityProbability)){
                        location = a.calculateNextLocation(broker.second, location, Random.nextDouble(0.0, 360.0))
                    }
                    timestamp += Random.nextInt(minPubTimeGap, maxPubTimeGap)
                }
                writer.flush()
                writer.close()
            }

            for (c in 0..broker.third.first) {

                if (a.workloadMachinePerBroker.get(b) == 0) {
                    a.logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                    break
                }
                currentWorkloadMachine = c % a.workloadMachinePerBroker.get(b)
                val clientName = randomName()
                a.logger.debug("Calculating actions for client $clientName")
                var location = Location.randomInGeofence(broker.second)
                var timestamp = Random.nextInt(0, 3000)
                val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Sub-$clientName.csv")
                val writer = file.bufferedWriter()
                writer.write(getHeader())
                writer.write(a.calculateSubscribeActions(timestamp, location))
                timestamp += 1000
                var subRenewalTime = Random.nextInt(minSubRenewalTime, maxSubRenewalTime)

                while (timestamp <= a.timeToRunPerClient) {

                    if (timestamp >= subRenewalTime) {
                        writer.write(a.calculateSubscribeActions(timestamp, location))
                        subRenewalTime += Random.nextInt(minSubRenewalTime, maxSubRenewalTime)
                    }
                    location = a.calculateNextLocation(broker.second, location, Random.nextDouble(0.0, 360.0))
                    writer.write(a.calculatePingActions(timestamp, location))
                    timestamp += Random.nextInt(minSubTimeGap, maxSubTimeGap)
                }
                writer.flush()
                writer.close()
            }


        }
        System.out.println(a.numberOfPublishedMessages)
        val distancePerClient = a.clientDistanceTravelled / (a.subsPerBrokerArea.stream().mapToInt { it }.sum() + a.brokerNames.size)
        val output = """Data set characteristics:
    Number of ping messages: ${a.numberOfPingMessages} (${(a.numberOfPingMessages) / (a.subsPerBrokerArea.stream().mapToInt { it }.sum() + a.brokerNames.size)} messages/per_subscriber)
    Number of subscribe messages: ${a.numberOfSubscribeMessages} (${a.numberOfSubscribeMessages / (a.subsPerBrokerArea.stream().mapToInt { it }.sum() + a.brokerNames.size)} messages/per_subscriber)
    Number of publish messages: ${a.numberOfPublishedMessages} (${a.numberOfPublishedMessages / (a.pubsPerBrokerArea.stream().mapToInt { it }.sum() + a.brokerNames.size)} messages/per_publisher)
    Publish payload size: ${a.totalPayloadSize / 1000.0} KB (${a.totalPayloadSize / a.numberOfPublishedMessages} byte/message)
    Client distance travelled: ${a.clientDistanceTravelled}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / a.timeToRunPerClient * 3600} km/h
    Number of message geofence broker overlaps: ${a.numberOfOverlappingMessageGeofences}"""

        a.logger.info(output)
        File("$directoryPath/02_summary.txt").appendText(output)
    }