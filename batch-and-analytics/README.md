### Planning Normalization

Let's start by outlining the database structure from our source data.

- meter_occupancy/
    - spaceid (Text)
    - eventtime (Floating Timestamp)
    - occupancystate (Text)
- parking_inventory_policies/
    - spaceid (Text)
    - metertype (Text)
    - ratetype (Text)
    - raterange (Text)
    - timelimit (Text)
    - blockface (Text)
    - latlng (Point or Location)

- parking_citations/
    - ticket_number (Text)
    - fine_amount (Number)
    - issue_date (Floating Timestamp)
    - issue_time (Text)
    - marked_time (Text)
    - violation_code (Text)
    - violation_description (Text)
    - agency (Text)
    - agency_desc (Text)
    - location (Text)
    - loc_lat (Number)
    - loc_long (Number)
    - geocodelocation (Point)
    - rp_state_plate (Text)
    - plate_expiry_date (Text / Number)
    - make (Text)
    - body_style (Text)
    - color (Text)
    - body_style_desc (Text)
    - color_desc (Text)

Then, we'll reorganize the schema for our analytical needs.

#### Dims:
- dim_vehicles (GROUP BY or SELECT DISTINCT to deduplicate)
    - MD5 primary key
    - rp_state_plate
    - plate_expiry_date
    - make
    - body_style_desc
    - color_desc

- dim_meter
    - meter_key (from fields spaceid, but we'll assume is same as meter_id)
    - meter_type
    - rate_type
    - rate_range
    - time_limit
    - location_key

- dim_location (lat/lan/geolocation) (GROUP BY or SELECT DISTINCT to deduplicate)
    - location_key
    - address_number_street (from fields location and blockface)
    - latitude
    - longitude

- dim_agency (GROUP BY or SELECT DISTINCT to deduplicate)
    - aagency_key VARCHAR PRIMARY KEY (agency field from source)
    - description
    
- dim_date (GROUP BY or SELECT DISTINCT to deduplicate)
    - date_key DATE PRIMARY KEY (native date key for fast partition pruning)
    - day
    - month
    - year

- dim_time_of_day (GROUP BY or SELECT DISTINCT to deduplicate)
    - time_key TIME PRIMARY KEY (native time key)
    - hour_number INT
    - minute_number INT

Violation (GROUP BY or SELECT DISTINCT to deduplicate)
    - MD5 Violoation key
    - code
    - description

Facts:
- Citations
    - ticket_number
    - fine_amount
    - Date_key
    - TimeOfDay_key
    - violation_key
    - agency_key
    - location_key
    - meter_key
    - vehicle_key

- Occupancy
    - occupancy_key
    - meter_key
    - Date_key
    - TimeOfDay_key

### Partitioning

`PARTITIONED BY` clause