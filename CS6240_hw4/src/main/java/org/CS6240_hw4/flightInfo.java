package org.CS6240_hw4;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class flightInfo implements Writable {

    int Year;
    int Quarter;
    int Month;
    int DayofMonth;
    int DayOfWeek;
    Text FlightDate;
    Text UniqueCarrier;
    int AirlineID;
    Text Carrier;
    Text TailNum;
    Text FlightNum;
    Text Origin;
    Text OriginCityName;
    Text OriginState;
    Text OriginStateFips;
    Text OriginStateName;
    float OriginWac;
    Text Dest;
    Text DestCityName;
    Text DestState;
    Text DestStateFips;
    Text DestStateName;
    float DestWac;
    Text CRSDepTime;
    Text DepTime;
    float DepDelay;
    float DepDelayMinutes;
    float DepDel15;
    float DepartureDelayGroups;
    Text DepTimeBlk;
    float TaxiOut;
    Text WheelsOff;
    Text WheelsOn;
    float TaxiIn;
    Text CRSArrTime;
    Text ArrTime;
    float ArrDelay;
    float ArrDelayMinutes;
    float ArrDel15;
    float ArrivalDelayGroups;
    Text ArrTimeBlk;
    float Cancelled;
    Text CancellationCode;
    float Diverted;
    float CRSElapsedTime;
    float ActualElapsedTime;
    float AirTime;
    float Flights;
    float Distance;
    float DistanceGroup;
    float CarrierDelay;
    float WeatherDelay;
    float NASDelay;
    float SecurityDelay;
    float LateAircraftDelay;

    public flightInfo() {

        FlightDate = new Text();
        UniqueCarrier = new Text();
        Carrier = new Text();
        TailNum = new Text();
        FlightNum = new Text();
        Origin = new Text();
        OriginCityName = new Text();
        OriginState = new Text();
        OriginStateFips = new Text();
        OriginStateName = new Text();
        Dest = new Text();
        DestCityName = new Text();
        DestState = new Text();
        DestStateFips = new Text();
        DestStateName = new Text();
        CRSDepTime = new Text();
        DepTime = new Text();
        DepTimeBlk = new Text();
        WheelsOff = new Text();
        WheelsOn = new Text();
        CRSArrTime = new Text();
        ArrTime = new Text();
        ArrTimeBlk = new Text();
        CancellationCode = new Text();

    }

    public float getYear() {
        return Year;
    }

    public void setYear(int year) {
        Year = year;
    }

    public float getQuarter() {
        return Quarter;
    }

    public void setQuarter(int quarter) {
        Quarter = quarter;
    }

    public float getMonth() {
        return Month;
    }

    public void setMonth(int month) {
        Month = month;
    }

    public float getDayofMonth() {
        return DayofMonth;
    }

    public void setDayofMonth(int dayofMonth) {
        DayofMonth = dayofMonth;
    }

    public float getDayOfWeek() {
        return DayOfWeek;
    }

    public void setDayOfWeek(int dayOfWeek) {
        DayOfWeek = dayOfWeek;
    }

    public Text getFlightDate() {
        return FlightDate;
    }

    public void setFlightDate(Text flightDate) {
        FlightDate = flightDate;
    }

    public Text getUniqueCarrier() {
        return UniqueCarrier;
    }

    public void setUniqueCarrier(Text uniqueCarrier) {
        UniqueCarrier = uniqueCarrier;
    }

    public float getAirlineID() {
        return AirlineID;
    }

    public void setAirlineID(int airlineID) {
        AirlineID = airlineID;
    }

    public Text getCarrier() {
        return Carrier;
    }

    public void setCarrier(Text carrier) {
        Carrier = carrier;
    }

    public Text getTailNum() {
        return TailNum;
    }

    public void setTailNum(Text tailNum) {
        TailNum = tailNum;
    }

    public Text getFlightNum() {
        return FlightNum;
    }

    public void setFlightNum(Text flightNum) {
        FlightNum = flightNum;
    }

    public Text getOrigin() {
        return Origin;
    }

    public void setOrigin(Text origin) {
        Origin = origin;
    }

    public Text getOriginCityName() {
        return OriginCityName;
    }

    public void setOriginCityName(Text originCityName) {
        OriginCityName = originCityName;
    }

    public Text getOriginState() {
        return OriginState;
    }

    public void setOriginState(Text originState) {
        OriginState = originState;
    }

    public Text getOriginStateFips() {
        return OriginStateFips;
    }

    public void setOriginStateFips(Text originStateFips) {
        OriginStateFips = originStateFips;
    }

    public Text getOriginStateName() {
        return OriginStateName;
    }

    public void setOriginStateName(Text originStateName) {
        OriginStateName = originStateName;
    }

    public float getOriginWac() {
        return OriginWac;
    }

    public void setOriginWac(float originWac) {
        OriginWac = originWac;
    }

    public Text getDest() {
        return Dest;
    }

    public void setDest(Text dest) {
        Dest = dest;
    }

    public Text getDestCityName() {
        return DestCityName;
    }

    public void setDestCityName(Text destCityName) {
        DestCityName = destCityName;
    }

    public Text getDestState() {
        return DestState;
    }

    public void setDestState(Text destState) {
        DestState = destState;
    }

    public Text getDestStateFips() {
        return DestStateFips;
    }

    public void setDestStateFips(Text destStateFips) {
        DestStateFips = destStateFips;
    }

    public Text getDestStateName() {
        return DestStateName;
    }

    public void setDestStateName(Text destStateName) {
        DestStateName = destStateName;
    }

    public float getDestWac() {
        return DestWac;
    }

    public void setDestWac(float destWac) {
        DestWac = destWac;
    }

    public Text getCRSDepTime() {
        return CRSDepTime;
    }

    public void setCRSDepTime(Text cRSDepTime) {
        CRSDepTime = cRSDepTime;
    }

    public Text getDepTime() {
        return DepTime;
    }

    public void setDepTime(Text depTime) {
        DepTime = depTime;
    }

    public float getDepDelay() {
        return DepDelay;
    }

    public void setDepDelay(float depDelay) {
        DepDelay = depDelay;
    }

    public float getDepDelayMinutes() {
        return DepDelayMinutes;
    }

    public void setDepDelayMinutes(float depDelayMinutes) {
        DepDelayMinutes = depDelayMinutes;
    }

    public float getDepDel15() {
        return DepDel15;
    }

    public void setDepDel15(float depDel15) {
        DepDel15 = depDel15;
    }

    public float getDepartureDelayGroups() {
        return DepartureDelayGroups;
    }

    public void setDepartureDelayGroups(float departureDelayGroups) {
        DepartureDelayGroups = departureDelayGroups;
    }

    public Text getDepTimeBlk() {
        return DepTimeBlk;
    }

    public void setDepTimeBlk(Text depTimeBlk) {
        DepTimeBlk = depTimeBlk;
    }

    public float getTaxiOut() {
        return TaxiOut;
    }

    public void setTaxiOut(float taxiOut) {
        TaxiOut = taxiOut;
    }

    public Text getWheelsOff() {
        return WheelsOff;
    }

    public void setWheelsOff(Text wheelsOff) {
        WheelsOff = wheelsOff;
    }

    public Text getWheelsOn() {
        return WheelsOn;
    }

    public void setWheelsOn(Text wheelsOn) {
        WheelsOn = wheelsOn;
    }

    public float getTaxiIn() {
        return TaxiIn;
    }

    public void setTaxiIn(float taxiIn) {
        TaxiIn = taxiIn;
    }

    public Text getCRSArrTime() {
        return CRSArrTime;
    }

    public void setCRSArrTime(Text cRSArrTime) {
        CRSArrTime = cRSArrTime;
    }

    public Text getArrTime() {
        return ArrTime;
    }

    public void setArrTime(Text arrTime) {
        ArrTime = arrTime;
    }

    public float getArrDelay() {
        return ArrDelay;
    }

    public void setArrDelay(float arrDelay) {
        ArrDelay = arrDelay;
    }

    public float getArrDelayMinutes() {
        return ArrDelayMinutes;
    }

    public void setArrDelayMinutes(float arrDelayMinutes) {
        ArrDelayMinutes = arrDelayMinutes;
    }

    public float getArrDel15() {
        return ArrDel15;
    }

    public void setArrDel15(float arrDel15) {
        ArrDel15 = arrDel15;
    }

    public float getArrivalDelayGroups() {
        return ArrivalDelayGroups;
    }

    public void setArrivalDelayGroups(float arrivalDelayGroups) {
        ArrivalDelayGroups = arrivalDelayGroups;
    }

    public Text getArrTimeBlk() {
        return ArrTimeBlk;
    }

    public void setArrTimeBlk(Text arrTimeBlk) {
        ArrTimeBlk = arrTimeBlk;
    }

    public float getCancelled() {
        return Cancelled;
    }

    public void setCancelled(float cancelled) {
        Cancelled = cancelled;
    }

    public Text getCancellationCode() {
        return CancellationCode;
    }

    public void setCancellationCode(Text cancellationCode) {
        CancellationCode = cancellationCode;
    }

    public float getDiverted() {
        return Diverted;
    }

    public void setDiverted(float diverted) {
        Diverted = diverted;
    }

    public float getCRSElapsedTime() {
        return CRSElapsedTime;
    }

    public void setCRSElapsedTime(float cRSElapsedTime) {
        CRSElapsedTime = cRSElapsedTime;
    }

    public float getActualElapsedTime() {
        return ActualElapsedTime;
    }

    public void setActualElapsedTime(float actualElapsedTime) {
        ActualElapsedTime = actualElapsedTime;
    }

    public float getAirTime() {
        return AirTime;
    }

    public void setAirTime(float airTime) {
        AirTime = airTime;
    }

    public float getFlights() {
        return Flights;
    }

    public void setFlights(float flights) {
        Flights = flights;
    }

    public float getDistance() {
        return Distance;
    }

    public void setDistance(float distance) {
        Distance = distance;
    }

    public float getDistanceGroup() {
        return DistanceGroup;
    }

    public void setDistanceGroup(float distanceGroup) {
        DistanceGroup = distanceGroup;
    }

    public float getCarrierDelay() {
        return CarrierDelay;
    }

    public void setCarrierDelay(float carrierDelay) {
        CarrierDelay = carrierDelay;
    }

    public float getWeatherDelay() {
        return WeatherDelay;
    }

    public void setWeatherDelay(float weatherDelay) {
        WeatherDelay = weatherDelay;
    }

    public float getNASDelay() {
        return NASDelay;
    }

    public void setNASDelay(float nASDelay) {
        NASDelay = nASDelay;
    }

    public float getSecurityDelay() {
        return SecurityDelay;
    }

    public void setSecurityDelay(float securityDelay) {
        SecurityDelay = securityDelay;
    }

    public float getLateAircraftDelay() {
        return LateAircraftDelay;
    }

    public void setLateAircraftDelay(float lateAircraftDelay) {
        LateAircraftDelay = lateAircraftDelay;
    }

    // Unimplemented methods of Interface Writable

    @Override
    public void readFields(DataInput in) throws IOException {
        Year = in.readInt();
        Quarter = in.readInt();
        Month = in.readInt();
        DayofMonth = in.readInt();
        DayOfWeek = in.readInt();

        FlightDate.readFields(in);
        UniqueCarrier.readFields(in);
        AirlineID = in.readInt();
        Carrier.readFields(in);
        TailNum.readFields(in);
        FlightNum.readFields(in);
        Origin.readFields(in);
        OriginCityName.readFields(in);
        OriginState.readFields(in);
        OriginStateFips.readFields(in);
        OriginStateName.readFields(in);
        OriginWac = in.readFloat();
        Dest.readFields(in);
        DestCityName.readFields(in);
        DestState.readFields(in);
        DestStateFips.readFields(in);
        DestStateName.readFields(in);
        DestWac = in.readFloat();
        CRSDepTime.readFields(in);
        DepTime.readFields(in);
        DepDelay = in.readFloat();
        DepDelayMinutes = in.readFloat();
        DepDel15 = in.readFloat();
        DepartureDelayGroups = in.readFloat();
        DepTimeBlk.readFields(in);
        TaxiOut = in.readFloat();
        WheelsOff.readFields(in);
        WheelsOn.readFields(in);
        TaxiIn = in.readFloat();
        CRSArrTime.readFields(in);
        ArrTime.readFields(in);
        ArrDelay = in.readFloat();
        ArrDelayMinutes = in.readFloat();
        ArrDel15 = in.readFloat();
        ArrivalDelayGroups = in.readFloat();
        ArrTimeBlk.readFields(in);
        Cancelled = in.readFloat();
        CancellationCode.readFields(in);
        Diverted = in.readFloat();
        CRSElapsedTime = in.readFloat();
        ActualElapsedTime = in.readFloat();
        AirTime = in.readFloat();
        Flights = in.readFloat();
        Distance = in.readFloat();
        DistanceGroup = in.readFloat();
        CarrierDelay = in.readFloat();
        WeatherDelay = in.readFloat();
        NASDelay = in.readFloat();
        SecurityDelay = in.readFloat();
        LateAircraftDelay = in.readFloat();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(Year);
        out.writeInt(Quarter);
        out.writeInt(Month);
        out.writeInt(DayofMonth);
        out.writeInt(DayOfWeek);
        FlightDate.write(out);
        UniqueCarrier.write(out);
        out.writeFloat(AirlineID);
        Carrier.write(out);
        TailNum.write(out);
        FlightNum.write(out);
        Origin.write(out);
        OriginCityName.write(out);
        OriginState.write(out);
        OriginStateFips.write(out);
        OriginStateName.write(out);
        out.writeFloat(OriginWac);
        Dest.write(out);
        DestCityName.write(out);
        DestState.write(out);
        DestStateFips.write(out);
        DestStateName.write(out);
        out.writeFloat(DestWac);
        CRSDepTime.write(out);
        DepTime.write(out);
        out.writeFloat(DepDelay);
        out.writeFloat(DepDelayMinutes);
        out.writeFloat(DepDel15);
        out.writeFloat(DepartureDelayGroups);
        DepTimeBlk.write(out);
        out.writeFloat(TaxiOut);
        WheelsOff.write(out);
        WheelsOn.write(out);
        out.writeFloat(TaxiIn);
        CRSArrTime.write(out);
        ArrTime.write(out);
        out.writeFloat(ArrDelay);
        out.writeFloat(ArrDelayMinutes);
        out.writeFloat(ArrDel15);
        out.writeFloat(ArrivalDelayGroups);
        ArrTimeBlk.write(out);
        out.writeFloat(Cancelled);
        CancellationCode.write(out);
        out.writeFloat(Diverted);
        out.writeFloat(CRSElapsedTime);
        out.writeFloat(ActualElapsedTime);
        out.writeFloat(AirTime);
        out.writeFloat(Flights);
        out.writeFloat(Distance);
        out.writeFloat(DistanceGroup);
        out.writeFloat(CarrierDelay);
        out.writeFloat(WeatherDelay);
        out.writeFloat(NASDelay);
        out.writeFloat(SecurityDelay);
        out.writeFloat(LateAircraftDelay);

    }

    public flightInfo(String flightData) {
        CSVParser csvParser = new CSVParser();
        String[] flightLine = null;
        try {
            flightLine = csvParser.parseLine(flightData);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < flightLine.length; i++) {
            if (flightLine[i].equals(null) || flightLine[i].equals("")) {
                flightLine[i] = "-10000.000";
            }
        }

        Year = Integer.parseInt(flightLine[0]);
        Quarter = Integer.parseInt(flightLine[1]);
        Month = Integer.parseInt(flightLine[2]);
        DayofMonth = Integer.parseInt(flightLine[3]);
        DayOfWeek = Integer.parseInt(flightLine[4]);
        FlightDate = new Text(flightLine[5]);
        UniqueCarrier = new Text(flightLine[6]);
        AirlineID = Integer.parseInt(flightLine[7]);
        Carrier = new Text(flightLine[8]);
        TailNum = new Text(flightLine[9]);
        FlightNum = new Text(flightLine[10]);
        Origin = new Text(flightLine[11]);
        OriginCityName = new Text(flightLine[12]);
        OriginState = new Text(flightLine[13]);
        OriginStateFips = new Text(flightLine[14]);
        OriginStateName = new Text(flightLine[15]);
        OriginWac = Float.parseFloat(flightLine[16]);
        Dest = new Text(flightLine[17]);
        DestCityName = new Text(flightLine[18]);
        DestState = new Text(flightLine[19]);
        DestStateFips = new Text(flightLine[20]);
        DestStateName = new Text(flightLine[21]);
        DestWac = Float.parseFloat(flightLine[22]);
        CRSDepTime = new Text(flightLine[23]);
        DepTime = new Text(flightLine[24]);
        DepDelay = Float.parseFloat(flightLine[25]);
        DepDelayMinutes = Float.parseFloat(flightLine[26]);
        DepDel15 = Float.parseFloat(flightLine[27]);
        DepartureDelayGroups = Float.parseFloat(flightLine[28]);
        DepTimeBlk = new Text(flightLine[29]);
        TaxiOut = Float.parseFloat(flightLine[30]);
        WheelsOff = new Text(flightLine[31]);
        WheelsOn = new Text(flightLine[32]);
        TaxiIn = Float.parseFloat(flightLine[33]);
        CRSArrTime = new Text(flightLine[34]);
        ArrTime = new Text(flightLine[35]);
        ArrDelay = Float.parseFloat(flightLine[36]);
        ArrDelayMinutes = Float.parseFloat(flightLine[37]);
        ArrDel15 = Float.parseFloat(flightLine[38]);
        ArrivalDelayGroups = Float.parseFloat(flightLine[39]);
        ArrTimeBlk = new Text(flightLine[40]);
        Cancelled = Float.parseFloat(flightLine[41]);
        CancellationCode = new Text(flightLine[42]);
        Diverted = Float.parseFloat(flightLine[43]);
        CRSElapsedTime = Float.parseFloat(flightLine[44]);
        ActualElapsedTime = Float.parseFloat(flightLine[45]);
        AirTime = Float.parseFloat(flightLine[46]);
        Flights = Float.parseFloat(flightLine[47]);
        Distance = Float.parseFloat(flightLine[48]);
        DistanceGroup = Float.parseFloat(flightLine[49]);
        CarrierDelay = Float.parseFloat(flightLine[50]);
        WeatherDelay = Float.parseFloat(flightLine[51]);
        NASDelay = Float.parseFloat(flightLine[52]);
        SecurityDelay = Float.parseFloat(flightLine[53]);
        LateAircraftDelay = Float.parseFloat(flightLine[54]);

    }

    public String toString() {
        String flightStr = Year + "," + Quarter + "," + Month + "," + DayofMonth + ","
                + DayOfWeek + "," + FlightDate + "," + UniqueCarrier + ","
                + AirlineID + "," + Carrier + "," + TailNum + "," + FlightNum
                + "," + Origin + "," + OriginCityName + "," + OriginState + ","
                + OriginStateFips + "," + OriginStateName + "," + OriginWac
                + "," + Dest + "," + DestCityName + "," + DestState + ","
                + DestStateFips + "," + DestStateName + "," + DestWac + ","
                + CRSDepTime + "," + DepTime + "," + DepDelay + ","
                + DepDelayMinutes + "," + DepDel15 + "," + DepartureDelayGroups
                + "," + DepTimeBlk + "," + TaxiOut + "," + WheelsOff + ","
                + WheelsOn + "," + TaxiIn + "," + CRSArrTime + "," + ArrTime
                + "," + ArrDelay + "," + ArrDelayMinutes + "," + ArrDel15 + ","
                + ArrivalDelayGroups + "," + ArrTimeBlk + "," + Cancelled + ","
                + CancellationCode + "," + Diverted + "," + CRSElapsedTime
                + "," + ActualElapsedTime + "," + AirTime + "," + Flights + ","
                + Distance + "," + DistanceGroup + "," + CarrierDelay + ","
                + WeatherDelay + "," + NASDelay + "," + SecurityDelay + ","
                + LateAircraftDelay;

        return flightStr;
    }

}