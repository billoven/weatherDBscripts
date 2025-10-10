#!/bin/bash

# Configuration
DB_NAME="WeatherStation"
DB_USER="weather_user"
DB_PASS="StrongPassword123"  # Change this to a secure password
DB_HOST="localhost"
NETWORK_ACCESS="192.168.17.%"
MYSQL_CMD="mysql -u root -p"

# Display configuration and request confirmation
echo "⚠️  This script will initialize the MySQL database with the following configuration:"
echo "------------------------------------------------------"
echo " Database Name  : $DB_NAME"
echo " Database User  : $DB_USER"
echo " Database Pass  : $DB_PASS"
echo " Database Host  : $DB_HOST"
echo " Network Access : $NETWORK_ACCESS"
echo "------------------------------------------------------"
read -p "❓ Do you want to proceed? (Y/N): " CONFIRM

# Convert input to lowercase
CONFIRM=$(echo "$CONFIRM" | tr '[:upper:]' '[:lower:]')

if [[ "$CONFIRM" != "y" && "$CONFIRM" != "yes" ]]; then
    echo "🚫 Operation cancelled. No changes were made."
    exit 0
fi

echo "🔹 Checking / Creating database '$DB_NAME'..."
$MYSQL_CMD <<EOF
CREATE DATABASE IF NOT EXISTS $DB_NAME;
EOF

if [ $? -ne 0 ]; then
    echo "❌ Error creating the database."
    exit 1
else
    echo "✅ Database checked/created successfully."
fi

echo "🔹 Checking / Creating tables..."
$MYSQL_CMD $DB_NAME <<EOF
CREATE TABLE IF NOT EXISTS DayWeatherConditions (
    WC_Date DATE NOT NULL PRIMARY KEY,
    WC_TempAvg DECIMAL(3,1),
    WC_TempHigh DECIMAL(3,1),
    WC_TempLow DECIMAL(3,1),
    WC_DewPointAvg DECIMAL(3,1),
    WC_DewPointHigh DECIMAL(3,1),
    WC_DewPointLow DECIMAL(3,1),
    WC_HumidityAvg INT(3),
    WC_HumidityHigh INT(3),
    WC_HumidityLow INT(3),
    WC_PressureAvg DECIMAL(5,1),
    WC_PressureHigh DECIMAL(5,1),
    WC_PressureLow DECIMAL(5,1),
    WC_WindSpeedMax DECIMAL(4,1),
    WC_GustSpeedMax DECIMAL(4,1),
    WC_PrecipitationSum DECIMAL(4,1),
    WC_SolarRadiationAvg DECIMAL(4,1)
);

CREATE TABLE IF NOT EXISTS WeatherConditions (
    WC_Datetime DATETIME NOT NULL PRIMARY KEY DEFAULT CURRENT_TIMESTAMP,
    WC_temp DECIMAL(3,1),
    WC_humidity INT(3),
    WC_precipRate DECIMAL(5,2),
    WC_precipTotal DECIMAL(6,2),
    WC_pressure DECIMAL(6,2),
    WC_heatIndex DECIMAL(3,1),
    WC_windSpeed DECIMAL(4,1),
    WC_windGust DECIMAL(4,1),
    WC_windChill DECIMAL(3,1),
    WC_winddir INT(3),
    WC_dewpt DECIMAL(3,1),
    WC_elev DECIMAL(4,1),
    WC_solarRadiation DECIMAL(5,1),
    WC_uv DECIMAL(2,1)
);

CREATE TABLE IF NOT EXISTS WeatherAlerts (
    id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    alert_name VARCHAR(255) NOT NULL UNIQUE,
    station_key VARCHAR(10),
    last_sent DATETIME NOT NULL,
    cooldown_minutes INT(11) NOT NULL
);
EOF

if [ $? -ne 0 ]; then
    echo "❌ Error creating tables."
    exit 1
else
    echo "✅ Tables checked/created successfully."
fi

echo "🔹 Checking / Creating user '$DB_USER'..."
$MYSQL_CMD <<EOF
CREATE USER IF NOT EXISTS '$DB_USER'@'$NETWORK_ACCESS' IDENTIFIED BY '$DB_PASS';
GRANT ALL PRIVILEGES ON $DB_NAME.* TO '$DB_USER'@'$NETWORK_ACCESS';
FLUSH PRIVILEGES;
EOF

if [ $? -ne 0 ]; then
    echo "❌ Error creating user '$DB_USER'."
    exit 1
else
    echo "✅ User '$DB_USER' created and granted permissions successfully."
fi

echo "🎉 Initialization completed successfully!"
exit 0
