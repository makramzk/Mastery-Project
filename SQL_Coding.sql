/* Step 1: Selecting the Cohort for Analysis

To ensure the reliability of our segmentation analysis, we first define a specific cohort of users.
This cohort includes users who have interacted with the platform after January 4, 2023, and have 
participated in more than seven browsing sessions.
*/
WITH session_based AS (
    SELECT 
        s.session_id,
        s.user_id,
        s.trip_id,
        s.session_start,
        s.session_end,
        s.flight_discount,
        s.hotel_discount,
        s.flight_discount_amount,
        s.hotel_discount_amount,
        s.flight_booked,
        s.hotel_booked,
  			CASE WHEN s.flight_booked IS TRUE OR s.hotel_booked IS TRUE THEN 1 ELSE 0 END AS fligh_or_hotel_booked,
  			CASE WHEN s.flight_booked IS TRUE AND s.hotel_booked IS TRUE THEN 1 ELSE 0 END AS fligh_and_hotel_booked,
        s.page_clicks,
        CASE WHEN s.cancellation IS TRUE THEN 1 ELSE 0 END AS cancellation,
        ROUND(EXTRACT(EPOCH FROM(s.session_end-s.session_start)),2) AS session_duration,
        f.origin_airport,
        f.destination,
        f.destination_airport,
        f.seats,
        f.return_flight_booked,
        f.departure_time,
        f.return_time,
        f.checked_bags,
        f.trip_airline,
        f.destination_airport_lat,
        f.destination_airport_lon,
        f.base_fare_usd,
        h.hotel_name,
        CASE WHEN h.nights < 0 THEN 1 ELSE h.nights END AS nights,
        h.rooms,
        h.check_in_time,
        h.check_out_time,
        h.hotel_per_room_usd AS hotel_price_per_room_night_USD,
        u.birthdate,
        /*CASE
            WHEN EXTRACT(YEAR FROM AGE(u.birthdate)) BETWEEN 16 AND 24 THEN '16-24'
            WHEN EXTRACT(YEAR FROM AGE(u.birthdate)) BETWEEN 25 AND 34 THEN '25-34'
            WHEN EXTRACT(YEAR FROM AGE(u.birthdate)) BETWEEN 35 AND 44 THEN '35-44'
            WHEN EXTRACT(YEAR FROM AGE(u.birthdate)) BETWEEN 45 AND 54 THEN '45-54'
            WHEN EXTRACT(YEAR FROM AGE(u.birthdate)) BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
          END AS age_group,*/
  			EXTRACT(YEAR FROM age(now(), u.birthdate)) AS age,
  			u.gender,
        CASE WHEN u.married IS TRUE THEN 'Married' ELSE 'Non_Married' END AS marital_status,
        CASE WHEN u.has_children IS TRUE THEN 'Yes' ELSE 'No' END AS has_children,
        u.home_country,
        u.home_city,
        u.home_airport,
        u.home_airport_lat,
        u.home_airport_lon,
        u.sign_up_date
    FROM sessions AS s
    LEFT JOIN flights AS f ON s.trip_id = f.trip_id
    LEFT JOIN hotels AS h ON s.trip_id = h.trip_id
    LEFT JOIN users AS u ON s.user_id = u.user_id
    WHERE s.user_id IN (SELECT user_id FROM sessions
                        WHERE session_start > '2023-01-04'
                        GROUP BY user_id
                        HAVING COUNT(*)> 7)  
),

/* Step 2: Aggregating Key Metrics for Each User

To group customers effectively, we need to aggregate key metrics such as session duration, page clicks, 
booking behaviors, and more. This provides insights into customer behavior and preferences.
*/
session_user_based AS( 
	SELECT 
  		sb.user_id,
  		SUM(sb.page_clicks) AS num_clicks,
  		COUNT(DISTINCT(session_id)) AS num_sessions,
      ROUND(AVG(session_duration),2) AS avg_session_duration,
  		CASE
      		WHEN SUM(sb.fligh_or_hotel_booked) = 0 THEN 0
      		ELSE SUM(sb.cancellation)::FLOAT / SUM(sb.fligh_or_hotel_booked)
    			END AS cancellation_rate,
    	CASE
      		WHEN SUM(sb.fligh_or_hotel_booked) = 0 THEN 0
      		ELSE SUM(sb.fligh_and_hotel_booked)::FLOAT / SUM(sb.fligh_or_hotel_booked)
    	END AS combined_booking
  
  FROM session_based AS sb
  GROUP BY sb.user_id
),

/* Additional flight and hotel-related metrics and 
   Handle possible negative values for nights in hotel bookings */


trip_based AS (
  SELECT
  		sb.user_id,
  		COUNT(trip_id) as num_trips,
  		SUM(CASE 	WHEN flight_booked IS TRUE AND return_flight_booked IS TRUE THEN 2
  							WHEN flight_booked IS TRUE THEN 1
  							ELSE 0
  							END) AS num_flights,
  		SUM((hotel_price_per_room_night_USD * nights*rooms) * (1 - COALESCE(hotel_discount_amount,0))) AS total_money_spent_hotel,
  		ROUND(AVG(EXTRACT(DAY FROM departure_time - session_end)),2) AS avg_time_before_trip,
  		ROUND(AVG(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon))::NUMERIC,2) AS avg_distance_flown,
      CASE
        WHEN COUNT(sb.flight_booked) = 0 THEN 0
        ELSE SUM(sb.checked_bags)::FLOAT / COUNT(sb.flight_booked)
      END AS avg_checked_bags,
      ROUND(AVG(sb.base_fare_usd), 2) AS avg_flight_price_usd,
      ROUND(AVG(sb.flight_discount_amount), 2) AS avg_flight_discount,
      ROUND(AVG(sb.seats), 2) AS avg_flight_seats,
      COUNT(DISTINCT CASE WHEN sb.hotel_booked IS TRUE THEN sb.trip_id END) AS total_hotels_booked,
      ROUND(AVG(sb.hotel_price_per_room_night_USD), 2) AS avg_hotel_price_usd,
      ROUND(AVG(sb.hotel_discount_amount), 2) AS avg_hotel_discount,
      ROUND(AVG(sb.rooms), 2) AS avg_hotel_rooms,
  		ROUND(AVG(EXTRACT(DAY from (sb.check_out_time - sb.check_in_time))), 2) AS avg_stay_duration_day
    FROM session_based AS sb
 		WHERE trip_id IS NOT NULL
  	AND trip_id NOT IN (SELECT distinct trip_id
                     		FROM session_based
                     		WHERE sb.cancellation :: boolean IS true)
		GROUP BY sb.user_id
),
/* Step 3: Define Customer's segment.Based on the aggregated metrics,I categorizes the customers into seven (7) distinct
segments. These segments will help in tailoring personalized rewards. */ 
 
customer_segmentation AS(
  SELECT 
  		sub.user_id,
  		CASE
 				  -- Seg. 1: 'Frequent Traveler' - Customers who book more trips than 75% of others.
      		WHEN tb.num_flights > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY num_flights) FROM trip_based) THEN 'Frequent Traveler'
  				WHEN tb.avg_flight_seats > 1  THEN 'Group Travelers' -- Seg.2: Customers with more than1 seats are categorized under segment Group Travelers.
  				WHEN sb.has_children = 'Yes' THEN 'Family Travelers' -- Seg.3: Customers who have children are categrized under Family Travelers segment. 
  				-- Seg.4: Customers Who are married and has no children are catagorized under the segment of Couple Travelers.
  				WHEN sb.marital_status = 'Married' AND sb.has_children = 'No' THEN 'Couple Travelers'
  				-- Seg. 5: Customers whos average stay of duration are less than 2 and average checked bags are equal or less than 1 are categrized under the Business Travelers.
  				WHEN tb.avg_stay_duration_day < 2 AND tb.avg_checked_bags <= 1 THEN 'Business Traveler'
  				WHEN sb.seats = 1 THEN 'Solo' --- Seg 6: Customers who booked one seat are categorized under the segment Solo.
  				ELSE 'Regular Travelers' END AS customer_segmentation --- Seg.7: The rest of customers who do not match the above creteria are categorized under Regular Travelers.
  FROM session_user_based AS sub
  LEFT JOIN trip_based AS tb ON sub.user_id = tb.user_id
  LEFT JOIN session_based AS sb ON sub.user_id = sb.user_id 
  
),
/* Step 4: Assign Perks to the Customers Segments. I assigned: 
1. No Cancellation Fee Perks for frequent Traveler
2. One Night Free Hotel with Flight for Group Travelers and Couple Travelers
3. Exclusive Discount for Family Travelers
4. Free checking bag for Business Traveler
5. Free Hotel Meal for Solo Traveler*/
perk_per_group AS(
  SELECT
			cs.user_id,
  		CASE 
  				WHEN cs.customer_segmentation = 'Frequent Traveler' THEN 'No Cancellation Fee'
  				WHEN cs.customer_segmentation = 'Group Travelers' THEN 'One Night Free Hotel with Flight'
  				WHEN cs.customer_segmentation = 'Family Travelers' THEN 'Exlcusive Discount'
  				WHEN cs.customer_segmentation = 'Couple Travelers' THEN 'One Night Free Hotel with Flight'
  				WHEN cs.customer_segmentation = 'Business Traveler' THEN 'Free Checking Bag'
  				WHEN cs.customer_segmentation = 'Solo' THEN 'Free hotel Meal'
  				ELSE 'Regular Traveler' END AS perk_per_group
  FROM customer_segmentation AS cs
)
/* Step 5: Join the tables together to have a complete customer profile*/

SELECT 
		DISTINCT ppg.user_id,
    sb.age,
    sub.num_clicks,
    sub.num_sessions,
    sub.avg_session_duration,
    sb.gender,
    sb.marital_status,
    sb.has_children,
    sb.home_country,
    sb.home_city,
    tb.num_trips,
    tb.num_flights,
    tb.total_money_spent_hotel,
    tb.avg_distance_flown,
    cs.customer_segmentation,
    ppg.perk_per_group
FROM perk_per_group AS ppg
LEFT JOIN session_based AS sb ON ppg.user_id = sb.user_id
LEFT JOIN session_user_based AS sub ON ppg.user_id = sub.user_id
LEFT JOIN trip_based AS tb ON  ppg.user_id = tb.user_id
LEFT JOIN customer_segmentation AS cs ON ppg.user_id = cs.user_id
WHERE num_trips IS NOT NULL 
AND num_flights IS NOT NULL
AND total_money_spent_hotel IS NOT NULL
AND avg_distance_flown IS NOT NULL
ORDER BY ppg.user_id  
 
