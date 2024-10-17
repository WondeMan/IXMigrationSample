using IXMIgrationSample.Model;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IXMIgrationSample.GetIXData
{

    class GetTicketDetail
    {
        public static string _connectionStringToOracle = ConfigurationManager.AppSettings["ConnectionStringToOracle"].ToString();
        public ResponseBO ComputeAdvancedPaxCount()
        {
            ResponseBO response = new ResponseBO();

            List<AdvancedPaxCount> advancedPaxCountList = new List<AdvancedPaxCount>();
            try
            {
                // get query 
                string query = GetPaxCountFromIXQuery();
                string SFquery = GetTkctDetailFromIXQuerySF();
                string Alphaquery = GetTkctDetailFromIXQuerySF();


                OracleConnection oracleConnection = new OracleConnection(_connectionStringToOracle);
                oracleConnection.Open();

                #region GetPax
                OracleCommand command = new OracleCommand(query, oracleConnection);
                OracleDataReader row = command.ExecuteReader();
                DateTime endFromIX = DateTime.Now;
                while (row.Read())
                {
                    AdvancedPaxCount advanced = new AdvancedPaxCount();

                    string depDate = row["sch_dep_Date"].ToString();

                    DateTime departureDate = DateTime.MinValue;
                    DateTime arivalDate = DateTime.MaxValue;

                    if (!string.IsNullOrEmpty(depDate) && !string.IsNullOrWhiteSpace(depDate))
                        departureDate = Convert.ToDateTime(depDate.Substring(0, 4) + "-" + depDate.Substring(4, 2) + "-" + depDate.Substring(6, 2));

                    advanced.Sch_Dep_Date = departureDate.ToString("ddMMMyy").ToUpper();
                    advanced.Sch_ArrivalDate = arivalDate.ToString("ddMMMyy").ToUpper();

                    advanced.SEC_ORIGN = row["SEC_ORIGN"].ToString();
                    advanced.SEC_DESTN = row["SEC_DESTN"].ToString();
                    advanced.FltNUm = row["OperFltNum"].ToString();
                    advanced.CABINCode = row["CabinCode"]?.ToString();
                    if (advanced.CABINCode != "Y" && advanced.CABINCode != "C")
                        advanced.CABINCode = "Y";

                    advanced.PAXCOUNT = row["PAXCOUNT"].ToString();

                    advancedPaxCountList.Add(advanced);
                }
                #endregion


                #region GetPax
                OracleCommand ixcommand = new OracleCommand(query, oracleConnection);
                OracleDataReader ixrow = command.ExecuteReader();
                while (row.Read())
                {
                    #region Mapping
                    //    { "BOOKING_DATE", reader["BOOKING_DATE"] },
                    //                            { "RLOC", reader["RLOC"] },
                    //                            { "NAME_COUNT", reader["NAME_COUNT"] },
                    //                            { "DATE_TICKET_ISSUED", reader["DATE_TICKET_ISSUED"] },
                    //                            { "DOCUMENT_FORM_TYPE", reader["DOCUMENT_FORM_TYPE"] },
                    //                            { "ISSUING_AGENT_GDS_OFFICE_CODE", reader["ISSUING_AGENT_GDS_OFFICE_CODE"] },
                    //                            { "ISSUING_AGENT_IATA_CODE", reader["ISSUING_AGENT_IATA_CODE"] },
                    //                            { "ISSUE_COUNTRY_CODE", reader["ISSUE_COUNTRY_CODE"] },
                    //                            { "ISSUE_CITY_CODE", reader["ISSUE_CITY_CODE"] },
                    //                            { "ISSUING_GDS_CODE", reader["ISSUING_GDS_CODE"] },
                    //                            { "ISSUING_GDS_RLOC", reader["ISSUING_GDS_RLOC"] },
                    //                            { "ORIG_ISSUE_CITY_CODE", reader["ORIG_ISSUE_CITY_CODE"] },
                    //                            { "ORIG_ISSUING_AGENT_IATA_CODE", reader["ORIG_ISSUING_AGENT_IATA_CODE"] },
                    //                            { "PASSENGER_NAME", reader["PASSENGER_NAME"] },
                    //                            { "TICKET_NUMBER", reader["TICKET_NUMBER"] },
                    //                            { "ORIG_TICKET_NUMBER", reader["ORIG_TICKET_NUMBER"] },
                    //                            { "TOTAL_FARE_AMOUNT", reader["TOTAL_FARE_AMOUNT"] },
                    //                            { "TOTAL_FARE_CURRENCY", reader["TOTAL_FARE_CURRENCY"] },
                    //                            { "TOTAL_AMOUNT", reader["TOTAL_AMOUNT"] },
                    //                            { "TOTAL_CURRENCY", reader["TOTAL_CURRENCY"] },
                    //                            { "NR_OF_CONJUNCTION_TKTS", reader["NR_OF_CONJUNCTION_TKTS"] },
                    //                            { "VALIDATING_CARRIER_CODE", reader["VALIDATING_CARRIER_CODE"] },
                    //                            { "COUPONS", new List<Dictionary<string, object>>() } 
                    #endregion
                }
                #endregion



                oracleConnection.Close();
            }
            catch (Exception ex)
            {

            }
            return response;
        }

        public string GetPaxCountFromIXQuery()
        {

            string query = @" select  distinct  sch_dep_Date,
                        sch_arrivalDate, --don't use this field as arrival date, we only use it for performance purpose
                        SEC_ORIGN, 
                        SEC_DESTN, 
                        OperFltNum,
                        CabinCode, 
                        count(CpnNum) as PaxCount
                        from ( SELECT distinct
                                tc.ISSUED_COMM_FLIGHT_NUMBER as MKTNGFltNum,
                               tc.ISSUED_OPER_FLIGHT_NUMBER as OPERFltNum,
                               tc.issued_operating_Carrier as OperCarrier,
                                tc.issued_commercial_carrier as CommCarrier,         
                                to_char(tc.ISSUED_DEPARTURE_DATE_TIME, 'YYYYMMDD') as sch_dep_Date,
                               to_char(tc.ISSUED_DEPARTURE_DATE_TIME,'YYYYMMDD') as sch_arrivalDate,
                               tc.issued_origin_airport as SEC_ORIGN,
                                tc.issued_destination_airport as SEC_DESTN,

                            case  
                             when  bni.CABIN_CODE = 'C' then 'C' 
                             else
                             'Y' end as CabinCode,
                             td.ticket_number as TicketDocument,
                                td.issue_country_code as IssueCountryCode,
                                to_char(td.date_ticket_issued, 'YYYYMMDD') as TktIssueDate,
                                TC.COUPON_NUMBER AS CpnNum         
                                FROM 
                                booking_name_item bni 
                                join booking_name bn on bn.booking_name_id = bni.booking_name_id
                                join booking bk on bk.booking_id = bn.booking_id                   
                                join ticket_document td on bk.rloc = td.rloc 
                                join ticket_coupon tc on tc.ticket_document_id = td.ticket_document_id 
                                WHERE   bni.item_status in (1,4,7)
                                        and bni.operating_booking_class not in('N','R')
                                       and TD.passenger_type not in('INF')
                                       and TD.document_form_type = 'T'                                     
                                       AND TC.COUPON_STATUS     IN ('Y','C','F','L','S')
                                    and  tc.issued_operating_Carrier = 'ET'
                                 and to_char(tc.ISSUED_DEPARTURE_DATE_TIME, 'YYYYMMDD') BETWEEN '" + DateTime.Now.AddDays(-4).ToString("yyyyMMdd") + "' AND '" + DateTime.Now.AddDays(6).ToString("yyyyMMdd") + "' " +
                                 " order by tc.coupon_number asc) group by  sch_dep_Date, sch_arrivalDate, SEC_ORIGN, SEC_DESTN,OperFltNum,CabinCode";


            return query;
        }

        public string GetTkctDetailFromIXQuerySF()
        {
            var query = @"SELECT DISTINCT
            B.BOOKING_DATE,
            B.RLOC,
            B.NAME_COUNT,
            TD.DATE_TICKET_ISSUED, 
            TD.DOCUMENT_FORM_TYPE, 
            TD.ISSUING_AGENT_GDS_OFFICE_CODE, 
            TD.ISSUING_AGENT_IATA_CODE, 
            TD.ISSUE_COUNTRY_CODE,
            TD.ISSUE_CITY_CODE, 
            TD.ISSUING_GDS_CODE, 
            TD.ISSUING_GDS_RLOC, 
            TD.ORIG_ISSUE_CITY_CODE, 
            TD.ORIG_ISSUING_AGENT_IATA_CODE, 
            TD.PASSENGER_NAME, 
            TD.TICKET_NUMBER,
            TD.ORIG_TICKET_NUMBER,
            TD.TOTAL_FARE_AMOUNT,
            TD.TOTAL_FARE_CURRENCY,
            TP.AMOUNT as TOTAL_AMOUNT, 
            TP.CURRENCY as TOTAL_CURRENCY, 
            TD.NR_OF_CONJUNCTION_TKTS,
            TD.VALIDATING_CARRIER_CODE,  
            TC.ENTITLEMENT_NUMBER as COUPON_NUMBER, 
            TC.COUPON_STATUS, 
            TC.ISSUED_COMM_FLIGHT_NUMBER, 
            TC.ISSUED_COMM_BOOKING_CLASS,
            TC.ISSUED_DEPARTURE_DATE_TIME, 
            TC.ISSUED_ARRIVAL_DATE_TIME, 
            TC.ISSUED_ORIGIN_AIRPORT, 
            TC.ISSUED_DESTINATION_AIRPORT, 
            TC.FARE_BASIS_CODE, 
            TC.ENDORSEMENTS_RESTRICTIONS, 
            TC.TICKET_DESIGNATOR, 
            TC.ISSUED_COMMERCIAL_CARRIER,
            TC.ISSUED_OPERATING_CARRIER,
            TC.FLOWN_BOOKING_CLASS,
            TD.FORM_OF_PAYMENT, 
            CASE
                WHEN BN.FQT_PROGRAM = 'ET' AND BN.FQT_LEVEL IN(0, 2, 32, 64, 4) THEN
                    CASE BN.FQT_LEVEL
                        WHEN 0 THEN 'ET Welcome'
                        WHEN 2 THEN 'ET Blue'
                        WHEN 32 THEN 'ET Silver'
                        WHEN 64 THEN 'ET Gold'
                        WHEN 4 THEN 'ET Platinium'
                    END
                WHEN BN.FQT_PROGRAM<> 'ET' AND BN.FQT_LEVEL IN(32, 2, 0) THEN+
                    CASE BN.FQT_LEVEL
                        WHEN 32 THEN 'Silver'
                        WHEN 2 THEN 'Gold'
                        WHEN 0 THEN 'Basic'
                    END
                ELSE 'Null'
            END AS TIER,
            BN.FQT_PROGRAM || '-' || BN.FQT_NUMBER AS MEMBER_ID,
            CASE
                WHEN(SELECT slc.value FROM service_line_component slc WHERE slc.booking_id = B.booking_id AND slc.name = 'EMAIL' AND ROWNUM = 1) IS NOT NULL THEN
                    (SELECT slc.value FROM service_line_component slc WHERE slc.booking_id= B.booking_id AND slc.name= 'EMAIL' AND ROWNUM = 1)
                ELSE
                    (SELECT SUBSTR(REPLACE(sl.free_text, ' ', ''), 14, 47) FROM service_line sl WHERE sl.booking_id = b.booking_id AND sl.free_text LIKE 'SSR CTCE%' AND ROWNUM = 1)
            END AS EMAIL,
            CASE
                WHEN(SELECT slc.value FROM service_line_component slc WHERE slc.booking_id = B.booking_id AND slc.name LIKE '%CDH_TEL_NUMBER%' AND ROWNUM = 1) IS NOT NULL THEN
                    (SELECT slc.value FROM service_line_component slc WHERE slc.booking_id= B.booking_id AND slc.name LIKE '%CDH_TEL_NUMBER%' AND ROWNUM = 1)
                WHEN LENGTH((SELECT slc.value FROM service_line_component slc WHERE slc.booking_id= B.booking_id AND slc.name LIKE '%PHONE%' AND ROWNUM = 1)) > 6 AND
                    (SELECT slc.value FROM service_line_component slc WHERE slc.booking_id = B.booking_id AND slc.name LIKE '%PHONE%' AND ROWNUM = 1) IS NOT NULL THEN
                    (SELECT slc.value FROM service_line_component slc WHERE slc.booking_id= B.booking_id AND slc.name LIKE '%PHONE%' AND ROWNUM = 1)
                ELSE
                    (SELECT SUBSTR(REPLACE(sl.free_text, ' ', ''), 14, 47) FROM service_line sl WHERE sl.booking_id = b.booking_id AND sl.free_text LIKE 'SSR CTC%' AND sl.secondary_type IN('CTCP', 'CTCM', 'CTCB', 'CTC', 'CTCH') AND ROWNUM = 1)
            END AS PHONE_NUMBER,
            DF.AIRCRAFT_TYPE,
            DP.SEAT_ROW || DP.SEAT_COLUMN AS SEAT,
            CASE
            WHEN DP.NUMBER_OF_BAGS IS NOT NULL THEN
            DP.NUMBER_OF_BAGS / DP.NUMBER_OF_BAGS
            ELSE DP.NUMBER_OF_BAGS
            END AS NUMBER_OF_BAGS,
             ROUND(
        CASE
        WHEN DP.NUMBER_OF_BAGS > 1 THEN DP.TOTAL_BAG_WEIGHT / DP.NUMBER_OF_BAGS
        ELSE DP.TOTAL_BAG_WEIGHT
        END, 2) AS TOTAL_BAG_WEIGHT,
            DPD.DOCUMENT_TYPE,
            DPD.DOCUMENT_NUMBER,
            TO_CHAR(DPD.DATE_OF_BIRTH, 'YYYY-MM-DD') AS DATE_OF_BIRTH,
            TO_CHAR(DPD.EXPIRATION_DATE, 'YYYY-MM-DD') AS EXPIRATION_DATE,
            DPD.ISSUING_COUNTRY,
            DPD.ISSUE_DATE,
            DPD.PASSENGER_COUNTRY,
            DPD.PLACE_OF_ISSUE,
            DPB.BAG_TAG_NUMBER
          FROM
            TICKET_DOCUMENT TD
            JOIN TICKET_COUPON TC ON TC.TICKET_DOCUMENT_ID = TD.TICKET_DOCUMENT_ID
            JOIN TICKET_DOCUMENT_LINK TDL ON TC.TICKET_COUPON_ID = TDL.TICKET_COUPON_ID
            LEFT JOIN TICKET_PAYMENT TP ON TD.TICKET_DOCUMENT_ID = TP.TICKET_DOCUMENT_ID AND TP.AMOUNT != 0
            JOIN BOOKING B ON TDL.BOOKING_ID = B.BOOKING_ID AND TD.RLOC = B.RLOC
            JOIN BOOKING_NAME BN ON B.BOOKING_ID = BN.BOOKING_ID
            LEFT JOIN SERVICE_LINE SL ON BN.BOOKING_NAME_ID = SL.BOOKING_NAME_ID AND SL.SERVICE_LINE_STATE NOT IN('0') AND SL.SERVICE_LINE_TYPE_CODE = 'SSR'
            JOIN BOOKING_NAME_ITEM BNI ON BNI.BOOKING_NAME_ID = BN.BOOKING_NAME_ID
            LEFT JOIN DCS_BOOKING_LINK DBL ON DBL.BOOKING_NAME_ITEM_ID = BNI.BOOKING_NAME_ITEM_ID
            LEFT JOIN DCS_FLIGHT_LEG DF ON DBL.DCS_FLIGHT_LEG_ID = DF.DCS_FLIGHT_LEG_ID AND(TC.ISSUED_COMM_FLIGHT_NUMBER = DF.COMMERCIAL_FLIGHT_NUMBER) AND DF.DCS_FLIGHT_LEG_ID IS NOT NULL
            LEFT JOIN DCS_PAX DP ON DF.DCS_FLIGHT_LEG_ID = DP.DCS_FLIGHT_LEG_ID AND DP.RLOC = TD.RLOC AND DP.FIRST_NAME = TD.FIRST_NAME AND DP.STATUS = 1
            LEFT JOIN DCS_PAX_DOC DPD ON DP.DCS_PAX_ID = DPD.DCS_PAX_ID AND DPD.DCS_FLIGHT_LEG_ID = DP.DCS_FLIGHT_LEG_ID AND DPD.STATUS NOT IN('0') AND DPD.DATE_OF_BIRTH IS NOT NULL
            LEFT JOIN DCS_PAX_BAG DPB ON DP.DCS_PAX_ID = DPB.DCS_PAX_ID AND DPB.DCS_FLIGHT_LEG_ID = DF.DCS_FLIGHT_LEG_ID AND DPD.STATUS = 1
          WHERE
            TD.TICKET_NUMBER = ;";


            return query;
        }

        public string GetAlphaFromIX()
        {
            var query = @"SELECT DISTINCT
        TD.RLOC AS PNR_LOCATOR, TD.FIRST_NAME,TD.LAST_NAME,
        CU.TITLE,CU.GENDER,TD.PASSENGER_TYPE AS PAX_TYPE,TD.TICKET_NUMBER,
        CASE
            WHEN TC.COUPON_STATUS IN ('Y') THEN 'OK'
            WHEN TC.COUPON_STATUS IN ('V') THEN 'VOIDED'
            WHEN TC.COUPON_STATUS IN ('F') THEN 'FLOWN'
            WHEN TC.COUPON_STATUS IN ('C') THEN 'CHKD-IN'
            WHEN TC.COUPON_STATUS IN ('L') THEN 'LIFTED'
            WHEN TC.COUPON_STATUS IN ('R') THEN 'RFNDED'
            WHEN TC.COUPON_STATUS IN ('E') THEN 'EXCH'
            WHEN TC.COUPON_STATUS IN ('S') THEN 'NOGO'
            WHEN TC.COUPON_STATUS IN ('N') THEN 'ARPT CTRL'
            ELSE 'NONE'
        END AS COUPON_STATUS,
        DPD.DATE_OF_BIRTH,
       -- CU.DATE_OF_BIRTH, --CU.NATIONALITY,
        (SELECT SLC.VALUE FROM service_line_component slc 
        WHERE slc.booking_id = bk.booking_id AND slc.name='EMAIL' 
        AND rownum = 1) AS Email,
        bn.contact_tel_num AS PHONE_NUMBER,
(SELECT MAX(SLN.FREE_TEXT)
FROM SERVICE_LINE SLN
JOIN BOOKING_NAME BNN ON BNN.BOOKING_NAME_ID = SLN.BOOKING_NAME_ID
WHERE SLN.SECONDARY_TYPE LIKE '%DOCS%' 
AND BNN.BOOKING_ID = BK.BOOKING_ID AND BNN.BOOKING_NAME_ID 
= BN.BOOKING_NAME_ID) AS TRAVEL_DOCUMENT,
DPD.DOCUMENT_NUMBER AS PSPT_NUM,
DPD.EXPIRATION_DATE AS PSPT_EXPR_DATE,
DPD.PASSENGER_COUNTRY AS PAX_NATIONALITY,
--,DPD.STATUS,DPD.DOCUMENT_TYPE,
TC.ISSUED_OPER_FLIGHT_NUMBER AS 
FLT_NUM,TC.ISSUED_ORIGIN_AIRPORT AS ORIGIN,
TC.ISSUED_DESTINATION_AIRPORT AS DESTINATION,
TC.ISSUED_ORIGIN_CITY 
||' '|| TC.ISSUED_DESTINATION_AIRPORT AS BOARDING_SEGMENT,
CASE
WHEN TC.ISSUED_COMM_BOOKING_CLASS IN ('C', 'D', 'J', 'P') AND SL.FREE_TEXT LIKE '%VIP %' THEN 'VIP_Business'
WHEN TC.ISSUED_COMM_BOOKING_CLASS IN ('C', 'D', 'J', 'P') THEN 'Business'
WHEN TC.ISSUED_COMM_BOOKING_CLASS IN ('R') THEN 'Staff_Business'
ELSE 'Economy'
END AS CABIN_CLASS,
TC.ISSUED_DEPARTURE_DATE_TIME 
AS DEPARTURE_DATE_TIME,TC.ISSUED_ARRIVAL_DATE_TIME AS ARRIVAL_DATE_TIME, 
CASE
    WHEN (
        SELECT ISSUED_DESTINATION_AIRPORT 
        FROM TICKET_COUPON 
        WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1 
        --AND ISSUED_ORIGIN_AIRPORT = 'ADD'
        AND 
        ticket_document_id = tc.ticket_document_id
    ) IS NOT NULL THEN 
        (SELECT ISSUED_DESTINATION_AIRPORT 
        FROM TICKET_COUPON 
        WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1 
       -- AND ISSUED_ORIGIN_AIRPORT = 'ADD'
        AND ticket_document_id = tc.ticket_document_id)
    ELSE 'N/A'
END AS NEXT_DESTINATION,
CASE
    WHEN (SELECT ISSUED_OPER_FLIGHT_NUMBER FROM TICKET_COUPON
          WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
          --AND ISSUED_ORIGIN_AIRPORT = 'ADD'
          AND ticket_document_id = tc.ticket_document_id) IS NULL
    THEN 'N/A'
    ELSE TO_CHAR((SELECT ISSUED_OPER_FLIGHT_NUMBER FROM TICKET_COUPON
                  WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
                  --AND ISSUED_ORIGIN_AIRPORT = 'ADD'
                  AND ticket_document_id = tc.ticket_document_id))
END AS CONN_FLT_NUM,
(TC.ISSUED_DESTINATION_AIRPORT ||' '||
CASE
    WHEN (
        SELECT ISSUED_DESTINATION_AIRPORT 
        FROM TICKET_COUPON 
        WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1 
        --AND ISSUED_ORIGIN_AIRPORT = 'ADD'
        AND 
        ticket_document_id = tc.ticket_document_id
    ) IS NOT NULL THEN 
        (SELECT ISSUED_DESTINATION_AIRPORT 
        FROM TICKET_COUPON 
        WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1 
       -- AND ISSUED_ORIGIN_AIRPORT = 'ADD'
        AND ticket_document_id = tc.ticket_document_id)
    ELSE ' '
END ) AS CONN_SEGMENT,
CASE
    WHEN (SELECT ISSUED_DEPARTURE_DATE_TIME FROM TICKET_COUPON
          WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
          --and ISSUED_ORIGIN_AIRPORT = 'ADD'
          AND ticket_document_id = tc.ticket_document_id) IS NULL
    THEN 'N/A'
    ELSE TO_CHAR((SELECT ISSUED_DEPARTURE_DATE_TIME FROM TICKET_COUPON
                  WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
                  --and ISSUED_ORIGIN_AIRPORT = 'ADD'
                  AND ticket_document_id = tc.ticket_document_id))
END AS CONN_DEPARTURE_DATE_TIME,
CASE
    WHEN (CAST((CAST((SELECT ISSUED_DEPARTURE_DATE_TIME FROM TICKET_COUPON
                      WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
                      --and ISSUED_ORIGIN_AIRPORT = 'ADD'
                      AND ticket_document_id = tc.ticket_document_id) AS DATE)
                  - CAST(TC.ISSUED_ARRIVAL_DATE_TIME AS DATE)) * 24 AS INT) IS NULL)
    THEN 'N/A'
    ELSE TO_CHAR(CAST((CAST((SELECT ISSUED_DEPARTURE_DATE_TIME FROM TICKET_COUPON
                            WHERE ENTITLEMENT_NUMBER = tc.ENTITLEMENT_NUMBER + 1
                            --and ISSUED_ORIGIN_AIRPORT = 'ADD'
                            AND ticket_document_id = tc.ticket_document_id) AS DATE)
                        - CAST(TC.ISSUED_ARRIVAL_DATE_TIME AS DATE)) * 24 AS INT))
END AS Stay_Hours,
 

    CASE
        WHEN BN.FQT_PROGRAM = 'ET' --AND STATUS = 1 AND FQT_NUMBER IS NOT NULL 
        THEN
            CASE
                WHEN FQT_LEVEL = 0 THEN FQT_PROGRAM || ' Welcome'
                WHEN FQT_LEVEL = 2 THEN FQT_PROGRAM || ' Blue'
                WHEN FQT_LEVEL = 32 THEN FQT_PROGRAM || ' Silver'
                WHEN FQT_LEVEL = 4 THEN FQT_PROGRAM || ' Platinum'
                WHEN FQT_LEVEL = 64 THEN FQT_PROGRAM || ' Welcome'
            END
        WHEN BN.FQT_PROGRAM NOT IN ('ET') --AND STATUS = 1 AND FQT_NUMBER IS NOT NULL 
        THEN
            CASE
                WHEN FQT_LEVEL = 0 THEN FQT_PROGRAM || ' Basic'
                WHEN FQT_LEVEL = 2 THEN FQT_PROGRAM || ' Gold'
                WHEN FQT_LEVEL = 32 THEN FQT_PROGRAM || ' Silver'
            END
        ELSE 'N/A'
    END AS FQTV_TIER
,
(SELECT MIN(SLNN.FREE_TEXT)
FROM SERVICE_LINE SLNN
JOIN BOOKING_NAME BNNN ON BNNN.BOOKING_NAME_ID = SLNN.BOOKING_NAME_ID
JOIN BOOKING_NAME_ITEM BNII ON BNII.BOOKING_NAME_ITEM_ID = SLNN.BOOKING_NAME_ITEM_ID
WHERE SLNN.SERVICE_LINE_TYPE_CODE ='SSR' 
AND BNNN.BOOKING_ID = BK.BOOKING_ID AND BNNN.BOOKING_NAME_ID = BN.BOOKING_NAME_ID
--AND SLNN.AIRLINE_CODE = SL.AIRLINE_CODE
AND SLNN.FREE_TEXT IS NOT NULL) AS SSR,
ROW_NUMBER() OVER (
        PARTITION BY TD.TICKET_NUMBER
        ORDER BY TD.RLOC, TD.TICKET_NUMBER
        ) AS RN
    FROM 
        TICKET_DOCUMENT TD
        INNER JOIN TICKET_COUPON TC 
        ON TC.TICKET_DOCUMENT_ID = TD.TICKET_DOCUMENT_ID
        INNER JOIN TICKET_DOCUMENT_LINK TDL 
        ON TDL.TICKET_COUPON_ID = TC.TICKET_COUPON_ID
        INNER JOIN BOOKING_NAME BN 
        ON BN.BOOKING_NAME_ID = TDL.BOOKING_NAME_ID
        INNER JOIN BOOKING BK ON BK.BOOKING_ID = BN.BOOKING_ID
        LEFT JOIN SERVICE_LINE SL ON (SL.BOOKING_ID = BK.BOOKING_ID AND SL.FREE_TEXT IS NOT NULL)
                    --       LEFT JOIN SERVICE_LINE_COMPONENT SLCO ON SLCO.SERVICE_LINE_ID = SL.SERVICE_LINE_ID
        LEFT JOIN NAME_LINK NL ON NL.BOOKING_NAME_ID = BN.BOOKING_NAME_ID
        LEFT JOIN CUSTOMER CU ON CU.CUSTOMER_ID = NL.CUSTOMER_ID
           LEFT JOIN DCS_BOOKING_LINK DBL 
           ON DBL.BOOKING_NAME_ID = BN.BOOKING_NAME_ID
           LEFT JOIN DCS_PAX_DOC DPD ON  (DPD.DCS_PAX_ID = DBL.DCS_PAX_ID AND DPD.DOCUMENT_NUMBER IS NOT NULL
           AND DPD.DOCUMENT_TYPE = 'P' and DPD.STATUS = 1 AND ROWNUM =1)
           --DPD.FIRST_NAME =  BN.FIRST_NAME AND DPD.LAST_NAME = BN.LAST_NAME AND        
    WHERE 
        TO_CHAR(TC.ISSUED_DEPARTURE_DATE_TIME, 
        'YYMMDD') BETWEEN '241017' AND '241017' 
        AND TC.ISSUED_OPER_FLIGHT_NUMBER = '852' 
        AND TC.COUPON_STATUS IN ('Y','F','C','L')
        --AND DPD.DOCUMENT_NUMBER IS NOT NULL
       -- AND DPD.DOCUMENT_TYPE = 'P' and DPD.STATUS = 1 AND DPD.PURGE_DATE > SYSDATE
        AND TD.DOCUMENT_FORM_TYPE IN ('T') ORDER BY TD.RLOC;";


            return query;
        }
    }
}
