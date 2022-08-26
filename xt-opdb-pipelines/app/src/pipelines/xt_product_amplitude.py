from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)


class XTProductAmplitude(XTDataPipeline):
    def __init__(self, search_size=10000) -> None:
        super().__init__(
            {
                "api_index": "prod_ihea_amplitude_analytics_index",
                "columns": [
                    {
                        "name": "AmplitudeEventType",
                        "json_path": "payload.amplitude_event_type",
                    },
                    {"name": "application", "json_path": "payload.app"},
                    {
                        "name": "BidPrice",
                        "json_path": "payload.event_properties.bidPrice",
                    },
                    {
                        "name": "BookPrice",
                        "json_path": "payload.event_properties.bookItPrice",
                    },
                    {
                        "name": "BookedLoadBrokerAccountDate",
                        "json_path": "payload.event_properties.load.bookingBroker.dateCreated",
                    },
                    {
                        "name": "BookedLoadBrokerAccountDisabledStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.disabled",
                    },
                    {
                        "name": "BookedLoadBrokerAccountType",
                        "json_path": "payload.event_properties.load.bookingBroker.accountType",
                    },
                    {
                        "name": "BookedLoadBrokerCompanyAdminStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.isCompanyAdmin",
                    },
                    {
                        "name": "BookedLoadBrokerElogAdminStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.isElogAdmin",
                    },
                    {
                        "name": "BookedLoadBrokerHaulynxAdminStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.isHaulynxAdmin",
                    },
                    {
                        "name": "BookedLoadBrokerJobTitle",
                        "json_path": "payload.event_properties.load.bookingBroker.jobTitle",
                    },
                    {
                        "name": "BookedLoadBrokerlastUpdate",
                        "json_path": "payload.event_properties.load.bookingBroker.lastUpdated",
                    },
                    {
                        "name": "BookedLoadBrokerName",
                        "json_path": "payload.event_properties.load.bookingBroker.broker.name",
                    },
                    {
                        "name": "BookedLoadBrokerSignupPlatform",
                        "json_path": "payload.event_properties.load.bookingBroker.signUp.platform",
                    },
                    {
                        "name": "BookedLoadBrokerSignupSource",
                        "json_path": "payload.event_properties.load.bookingBroker.signUp.source",
                    },
                    {
                        "name": "BookedLoadBrokerStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.broker.isHaulynxBroker",
                    },
                    {
                        "name": "BookedLoadBrokerTeam",
                        "json_path": "payload.event_properties.load.bookingBroker.team",
                    },
                    {
                        "name": "BookedLoadBrokerVerificationStatus",
                        "json_path": "payload.event_properties.load.bookingBroker.isVerified",
                    },
                    {
                        "name": "BookedLoadStatus",
                        "json_path": "payload.event_properties.load.bookStatus",
                    },
                    {
                        "name": "BookingMethod",
                        "json_path": "payload.event_properties.bookingMethod",
                    },
                    {
                        "name": "brokerAcceptedCarrier",
                        "json_path": "payload.event_properties.load.brokerAcceptedCarrier",
                    },
                    {
                        "name": "BrokerID",
                        "json_path": "payload.event_properties.broker",
                    },
                    {
                        "name": "carrierAccepted",
                        "json_path": "payload.event_properties.load.carrierAccepted",
                    },
                    {
                        "name": "CarrierDomicileCity",
                        "json_path": "payload.event_properties.load.carrier.addressCity",
                    },
                    {
                        "name": "CarrierDomicileCountry",
                        "json_path": "payload.event_properties.load.carrier.addressCountry",
                    },
                    {
                        "name": "CarrierDomicileState",
                        "json_path": "payload.event_properties.load.carrier.addressState",
                    },
                    {
                        "name": "CarrierDOT",
                        "json_path": "payload.event_properties.carrierDot",
                    },
                    {
                        "name": "CarrierELDStatus",
                        "json_path": "payload.event_properties.load.carrier.isEldCarrier",
                    },
                    {
                        "name": "CarrierEquipmentType",
                        "json_path": "payload.event_properties.load.carrier.equipmentType",
                    },
                    {
                        "name": "CarrierHaulynxStatus",
                        "json_path": "payload.event_properties.load.carrier.isHaulynxCarrier",
                    },
                    {
                        "name": "CarrierHaulynxUserStatus",
                        "json_path": "payload.event_properties.load.carrier.isHaulynxUser",
                    },
                    {
                        "name": "CarrierName",
                        "json_path": "payload.event_properties.carrierName",
                    },
                    {
                        "name": "CarrierP44status",
                        "json_path": "payload.event_properties.isp44",
                    },
                    {
                        "name": "CarrierRMISSetupStatus",
                        "json_path": "payload.event_properties.load.carrier.isRmisSetup",
                    },
                    {
                        "name": "CarrierUSXCertidicationStatus",
                        "json_path": "payload.event_properties.load.carrier.status.keyword",
                    },
                    {
                        "name": "CarrierUSXCertifiedStatus",
                        "json_path": "payload.event_properties.load.carrier.isUSXCertified",
                    },
                    {
                        "name": "ClickedCarrierAddress",
                        "json_path": "payload.event_properties.clickedCarrier.addressCity",
                    },
                    {
                        "name": "ClickedCarrierCountry",
                        "json_path": "payload.event_properties.clickedCarrier.addressCountry",
                    },
                    {
                        "name": "ClickedCarrierDOT",
                        "json_path": "payload.event_properties.clickedCarrier.dot",
                    },
                    {
                        "name": "ClickedCarrierELDstatus",
                        "json_path": "payload.event_properties.clickedCarrier.isEldCarrier",
                    },
                    {
                        "name": "ClickedCarrierEquimentType",
                        "json_path": "payload.event_properties.clickedCarrier.equipmentType",
                    },
                    {
                        "name": "ClickedCarrierLDstatus",
                        "json_path": "payload.event_properties.clickedCarrier.isLoadDistributionEnabled",
                    },
                    {
                        "name": "ClickedCarrierLFDate",
                        "json_path": "payload.event_properties.clickedCarrier.loadFeedEnabledDate",
                    },
                    {
                        "name": "ClickedCarrierMaxDeadMiles",
                        "json_path": "payload.event_properties.clickedCarrier.maximumDeadMiles",
                    },
                    {
                        "name": "ClickedCarrierMaxLoadValue",
                        "json_path": "payload.event_properties.clickedCarrier.minimumLoadValue",
                    },
                    {
                        "name": "ClickedCarrierOwner",
                        "json_path": "payload.event_properties.clickedCarrier.owner",
                    },
                    {
                        "name": "ClickedCarrierRank",
                        "json_path": "payload.event_properties.clickedCarrier.rank",
                    },
                    {
                        "name": "ClickedCarrierState",
                        "json_path": "payload.event_properties.clickedCarrier.addressState",
                    },
                    {
                        "name": "ClickedCarrierUserstatus",
                        "json_path": "payload.event_properties.clickedCarrier.isHaulynxUser",
                    },
                    {
                        "name": "ClientEventTime",
                        "json_path": "payload.client_event_time",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {
                        "name": "ClientUploadTime",
                        "json_path": "payload.client_upload_time",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {
                        "name": "CompanyAdmin",
                        "json_path": "payload.user_properties.isCompanyAdmin",
                    },
                    {
                        "name": "ConsigneeName",
                        "json_path": "payload.event_properties.load.consignee",
                    },
                    {
                        "name": "CustomerName",
                        "json_path": "payload.event_properties.load.locations.customer.name",
                    },
                    {
                        "name": "CustomerNumber",
                        "json_path": "payload.event_properties.load.customer",
                    },
                    {
                        "name": "Destination",
                        "json_path": "payload.event_properties.destination",
                    },
                    {
                        "name": "DestinationRadius",
                        "json_path": "payload.event_properties.destinationRadius",
                        "conversion": XTDataPipelineConversion.Radius,
                    },
                    {
                        "name": "DetailType",
                        "json_path": "payload.event_properties.load.provider.detailType",
                    },
                    {
                        "name": "DriverAccountType",
                        "json_path": "payload.event_properties.load.driver.accountType",
                    },
                    {
                        "name": "DriversCarrierDOT",
                        "json_path": "payload.event_properties.load.driver.carrier.dot",
                    },
                    {
                        "name": "DriverDisabledStatus",
                        "json_path": "payload.event_properties.load.driver.disabled",
                    },
                    {
                        "name": "DriverDomicileCity",
                        "json_path": "payload.event_properties.load.driver.carrier.addressCity",
                    },
                    {
                        "name": "DriverDomicileCountry",
                        "json_path": "payload.event_properties.load.driver.carrier.addressCountry",
                    },
                    {
                        "name": "DriverDomicileState",
                        "json_path": "payload.event_properties.load.driver.carrier.addressState",
                    },
                    {
                        "name": "DriverSignupDate",
                        "json_path": "payload.event_properties.load.driver.dateCreated",
                    },
                    {
                        "name": "DriverSignupPlatform",
                        "json_path": "payload.event_properties.load.driver.signUp.platform",
                    },
                    {
                        "name": "DriverVerificationStatus",
                        "json_path": "payload.event_properties.load.driver.isVerified",
                    },
                    {
                        "name": "EarlyDelivery",
                        "json_path": "payload.event_properties.load.loadLocations.customerAttributes.canDeliverEarly",
                    },
                    {
                        "name": "EarlyPickup",
                        "json_path": "payload.event_properties.load.loadLocations.customerAttributes.canPickUpEarly",
                    },
                    {
                        "name": "entity_timestamp",
                        "json_path": "meta.entity_timestamp",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {
                        "name": "entity_timestamp_ms",
                        "json_path": "meta.entity_timestamp_ms",
                    },
                    {
                        "name": "EquipmentType",
                        "json_path": "payload.event_properties.equipmentType",
                    },
                    {
                        "name": "EventCarrierDOT",
                        "json_path": "payload.event_properties.load.carrier.dot",
                    },
                    {"name": "EventCity", "json_path": "payload.city"},
                    {"name": "EventCountry", "json_path": "payload.country"},
                    {
                        "name": "EventCustomerName",
                        "json_path": "payload.event_properties.load.customerName",
                    },
                    {"name": "EventMetropolitanArea", "json_path": "payload.dma"},
                    {
                        "name": "EventTime",
                        "json_path": "payload.event_time",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {"name": "EventType", "json_path": "payload.event_type"},
                    {
                        "name": "ExclusiveLoad",
                        "json_path": "payload.event_properties.load.exclusiveLoad",
                    },
                    {
                        "name": "ExclusivePrice",
                        "json_path": "payload.event_properties.load.exclusivePrice",
                    },
                    {
                        "name": "laneRecommendations",
                        "json_path": "payload.event_properties.laneRecommendations",
                    },
                    {"name": "Language", "json_path": "payload.language"},
                    {
                        "name": "Load/UnloadStatus",
                        "json_path": "payload.event_properties.status",
                    },
                    {
                        "name": "LoadBookingTime",
                        "json_path": "payload.event_properties.load.paymentDetails.bookedAt",
                    },
                    {
                        "name": "LoadCompletionDate",
                        "json_path": "payload.event_properties.load.dateCompleted",
                    },
                    {
                        "name": "LoadCreationDate",
                        "json_path": "payload.event_properties.load.dateCreated.keyword",
                    },
                    {
                        "name": "LoadDistance",
                        "json_path": "payload.event_properties.load.distance",
                    },
                    {
                        "name": "LoadFinalledStatus",
                        "json_path": "payload.event_properties.load.finalled",
                    },
                    {"name": "LoadID", "json_path": "payload.event_properties.loadId"},
                    {
                        "name": "LoadOrderNumber",
                        "json_path": "payload.event_properties.loadOrderNum",
                    },
                    {
                        "name": "LoadPaymentMiles",
                        "json_path": "payload.event_properties.load.paymentDetails.distanceMiles",
                    },
                    {
                        "name": "LoadRateperMile",
                        "json_path": "payload.event_properties.load.ratePerMile",
                    },
                    {
                        "name": "LoadRevenue",
                        "json_path": "payload.event_properties.load.paymentDetails.revenue",
                    },
                    {
                        "name": "LoadTruckType",
                        "json_path": "payload.event_properties.load.truck.type",
                    },
                    {
                        "name": "LowestOfferCarrierName",
                        "json_path": "payload.event_properties.load.bidDetails.lowestOfferCarrierName",
                    },
                    {
                        "name": "MilesCalculationType",
                        "json_path": "payload.event_properties.load.truck.mileage.calculationType",
                    },
                    {
                        "name": "OffersCount",
                        "json_path": "payload.event_properties.load.bidDetails.offerCount",
                    },
                    {"name": "Origin", "json_path": "payload.event_properties.origin"},
                    {
                        "name": "OriginRadius",
                        "json_path": "payload.event_properties.originRadius",
                        "conversion": XTDataPipelineConversion.Radius,
                    },
                    {
                        "name": "Page/TabName",
                        "json_path": "payload.event_properties.selectedTab",
                    },
                    {
                        "name": "PickupEndingDate",
                        "json_path": "payload.event_properties.pickupDateRangeEnd",
                        "conversion": XTDataPipelineConversion.Date,
                    },
                    {
                        "name": "PickupStartingDate",
                        "json_path": "payload.event_properties.pickupDateRangeStart",
                        "conversion": XTDataPipelineConversion.Date,
                    },
                    {"name": "Platform", "json_path": "payload.platform"},
                    {
                        "name": "ProcessedTime",
                        "json_path": "payload.processed_time",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {
                        "name": "quantity",
                        "json_path": "payload.event_properties.load.loadLocations.quantity",
                    },
                    {
                        "name": "QueryforassociatedBids",
                        "json_path": "payload.event_properties.router.queryParams.includeBids",
                    },
                    {
                        "name": "QueryforBookingStatus",
                        "json_path": "payload.event_properties.router.queryParams.bookStatus",
                    },
                    {
                        "name": "QueryforBrokerTeam",
                        "json_path": "payload.event_properties.router.queryParams.brokerTeamId",
                    },
                    {
                        "name": "QueryforCarrierDOT",
                        "json_path": "payload.event_properties.router.queryParams.carrierNameOrDot",
                    },
                    {
                        "name": "QueryforLoadStatus",
                        "json_path": "payload.event_properties.router.queryParams.loadStatus",
                    },
                    {
                        "name": "QueryforMaxDistance",
                        "json_path": "payload.event_properties.router.queryParams.maxDistance",
                    },
                    {
                        "name": "QueryforminDistance",
                        "json_path": "payload.event_properties.router.queryParams.minDistance",
                    },
                    {
                        "name": "QueryforOrderType",
                        "json_path": "payload.event_properties.router.queryParams.orderType",
                    },
                    {
                        "name": "QueryforOrigin",
                        "json_path": "payload.event_properties.router.queryParams.origin",
                    },
                    {
                        "name": "QueryforRadius",
                        "json_path": "payload.event_properties.router.queryParams.radius",
                    },
                    {
                        "name": "QueryforReferrals",
                        "json_path": "payload.event_properties.router.queryParams.utm_source",
                    },
                    {
                        "name": "QueryforRegion",
                        "json_path": "payload.event_properties.router.queryParams.region",
                    },
                    {
                        "name": "QueryforShipper",
                        "json_path": "payload.event_properties.searchQuery.shipper",
                    },
                    {"name": "Region", "json_path": "payload.region"},
                    {
                        "name": "RejectedLoad",
                        "json_path": "payload.event_properties.load.loadRejectedByAllCarriers",
                    },
                    {
                        "name": "StopsAllowedbyEvent",
                        "json_path": "payload.event_properties.allowStops",
                    },
                    {
                        "name": "TenderType",
                        "json_path": "payload.event_properties.load.orderType",
                    },
                    {
                        "name": "TrackingType",
                        "json_path": "payload.event_properties.trackingType",
                    },
                    {
                        "name": "TrailersCarriersCity",
                        "json_path": "payload.event_properties.load.trailer.carrier.addressCity",
                    },
                    {
                        "name": "TrailersCarriersName",
                        "json_path": "payload.event_properties.load.trailer.carrier.saferWatchData.name",
                    },
                    {
                        "name": "TrailerName",
                        "json_path": "payload.event_properties.load.trailer.__typename.keyword",
                    },
                    {
                        "name": "TruckDispatchTime",
                        "json_path": "payload.event_properties.load.dispatchLocation.timestamp",
                    },
                    {
                        "name": "TruckDispatchTimeUpdate",
                        "json_path": "payload.event_properties.load.dispatchLocation.updatedAt",
                    },
                    {
                        "name": "TruckEngineStatus",
                        "json_path": "payload.event_properties.load.truck.truckState",
                    },
                    {
                        "name": "TruckEstimatedWaitTime",
                        "json_path": "payload.event_properties.load.loadLocations.estimatedWaitTime",
                    },
                    {
                        "name": "TruckFacilityDepartureTime",
                        "json_path": "payload.event_properties.load.loadLocations.departureTime",
                    },
                    {
                        "name": "TruckFacilityEntranceTime",
                        "json_path": "payload.event_properties.load.loadLocations.entranceTime",
                    },
                    {
                        "name": "TruckLocationType",
                        "json_path": "payload.event_properties.load.loadLocations.locationType",
                    },
                    {
                        "name": "TrucksCarriersName",
                        "json_path": "payload.event_properties.load.truck.carrierId",
                    },
                    {
                        "name": "TrucksDutyStatus",
                        "json_path": "payload.event_properties.load.truck.dutyStatus",
                    },
                    {
                        "name": "TypeofMilestone",
                        "json_path": "payload.event_properties.milestoneType",
                    },
                    {
                        "name": "TypeofMilestoneUpdate",
                        "json_path": "payload.event_properties.update",
                    },
                    {
                        "name": "UserAccountType",
                        "json_path": "payload.user_properties.accountType",
                    },
                    {
                        "name": "UserCarrierDOT",
                        "json_path": "payload.user_properties.dotNumber",
                    },
                    {
                        "name": "UserJobTitle",
                        "json_path": "payload.user_properties.userJobTitle",
                    },
                    {
                        "name": "UserManager",
                        "json_path": "payload.user_properties.userManager",
                    },
                    {
                        "name": "UserSignup",
                        "json_path": "payload.user_creation_time",
                        "conversion": XTDataPipelineConversion.ESTimestamp,
                    },
                    {
                        "name": "UsersCarrier",
                        "json_path": "payload.user_properties.haulynxCarrier",
                    },
                    {
                        "name": "UserTeam",
                        "json_path": "payload.user_properties.userTeam",
                    },
                ],
                "db_table_name": "XNS_reporting.dbo.XT_Product_Amplitude",
            },
            search_size,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_entity_timestamp_ms = 0

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(entity_timestamp_ms) FROM {self.db_table_name}"
                cursor.execute(sql)

                for row in cursor.fetchall():
                    if row[0] is not None:
                        max_entity_timestamp_ms = int(row[0])

            for api_row in self.search(
                "meta.entity_timestamp_ms", max_entity_timestamp_ms
            ):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
