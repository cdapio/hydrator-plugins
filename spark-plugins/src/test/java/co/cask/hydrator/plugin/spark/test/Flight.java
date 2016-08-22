/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package co.cask.hydrator.plugin.spark.test;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class Flight {
  private final Schema schema =
    Schema.recordOf("flightData", Schema.Field.of("dofM", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("dofW", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("carrier", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("tailNum", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("flightNum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("originID", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("origin", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("destId", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("dest", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("scheduleDepTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("deptime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("depDelayMins", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("scheduledArrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrDelay", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("elapsedTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("distance", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("delayed", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
  public int dofM;
  public String carrier;
  private int dofW;
  private String tailNum;
  private int flightNum;
  private String originID;
  private String origin;
  private String destId;
  private String dest;
  private double scheduleDepTime;
  private Double deptime;
  private Double depDelayMins;
  private Double scheduledArrTime;
  private Double arrTime;
  private Double arrDelay;
  private Double elapsedTime;
  private int distance;
  private Double delayed;

  public Flight(int dofM, int dofW, String carrier, String tailNum, int flightNum, String originID, String origin,
                String destId, String dest, double scheduleDepTime, Double deptime, Double depDelayMins,
                Double scheduledArrTime, Double arrTime, Double arrDelay, Double elapsedTime, int distance,
                double delayed) {
    this.dofM = dofM;
    this.dofW = dofW;
    this.carrier = carrier;
    this.tailNum = tailNum;
    this.flightNum = flightNum;
    this.originID = originID;
    this.origin = origin;
    this.destId = destId;
    this.dest = dest;
    this.scheduleDepTime = scheduleDepTime;
    this.deptime = deptime;
    this.depDelayMins = depDelayMins;
    this.scheduledArrTime = scheduledArrTime;
    this.arrTime = arrTime;
    this.arrDelay = arrDelay;
    this.elapsedTime = elapsedTime;
    this.distance = distance;
    this.delayed = delayed;
  }

  public Flight(int dofM, int dofW, String carrier, String tailNum, int flightNum, String originID, String origin,
                String destId, String dest, double scheduleDepTime, Double deptime, Double depDelayMins,
                Double scheduledArrTime, Double arrTime, Double arrDelay, Double elapsedTime, int distance) {
    this.dofM = dofM;
    this.dofW = dofW;
    this.carrier = carrier;
    this.tailNum = tailNum;
    this.flightNum = flightNum;
    this.originID = originID;
    this.origin = origin;
    this.destId = destId;
    this.dest = dest;
    this.scheduleDepTime = scheduleDepTime;
    this.deptime = deptime;
    this.depDelayMins = depDelayMins;
    this.scheduledArrTime = scheduledArrTime;
    this.arrTime = arrTime;
    this.arrDelay = arrDelay;
    this.elapsedTime = elapsedTime;
    this.distance = distance;
    this.delayed = null;
  }

  public static Flight fromStructuredRecord(StructuredRecord structuredRecord) {
    return new Flight((Integer) structuredRecord.get("dofM"),
                      (Integer) structuredRecord.get("dofW"),
                      (String) structuredRecord.get("carrier"),
                      (String) structuredRecord.get("tailNum"),
                      (Integer) structuredRecord.get("flightNum"),
                      (String) structuredRecord.get("originID"),
                      (String) structuredRecord.get("origin"),
                      (String) structuredRecord.get("destId"),
                      (String) structuredRecord.get("dest"),
                      (Double) structuredRecord.get("scheduleDepTime"),
                      (Double) structuredRecord.get("deptime"),
                      (Double) structuredRecord.get("depDelayMins"),
                      (Double) structuredRecord.get("scheduledArrTime"),
                      (Double) structuredRecord.get("arrTime"),
                      (Double) structuredRecord.get("arrDelay"),
                      (Double) structuredRecord.get("elapsedTime"),
                      (Integer) structuredRecord.get("distance"),
                      (Double) structuredRecord.get("delayed"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Flight)) {
      return false;
    }

    Flight flight = (Flight) o;

    if (dofM != flight.dofM) {
      return false;
    }
    if (dofW != flight.dofW) {
      return false;
    }
    if (flightNum != flight.flightNum) {
      return false;
    }
    if (Double.compare(flight.scheduleDepTime, scheduleDepTime) != 0) {
      return false;
    }
    if (distance != flight.distance) {
      return false;
    }
    if (!carrier.equals(flight.carrier)) {
      return false;
    }
    if (tailNum != null ? !tailNum.equals(flight.tailNum) : flight.tailNum != null) {
      return false;
    }
    if (originID != null ? !originID.equals(flight.originID) : flight.originID != null) {
      return false;
    }
    if (!origin.equals(flight.origin)) {
      return false;
    }
    if (destId != null ? !destId.equals(flight.destId) : flight.destId != null) {
      return false;
    }
    if (!dest.equals(flight.dest)) {
      return false;
    }
    if (deptime != null ? !deptime.equals(flight.deptime) : flight.deptime != null) {
      return false;
    }
    if (!depDelayMins.equals(flight.depDelayMins)) {
      return false;
    }
    if (scheduledArrTime != null ? !scheduledArrTime.equals(flight.scheduledArrTime) :
      flight.scheduledArrTime != null) {
      return false;
    }
    if (arrTime != null ? !arrTime.equals(flight.arrTime) : flight.arrTime != null) {
      return false;
    }
    if (arrDelay != null ? !arrDelay.equals(flight.arrDelay) : flight.arrDelay != null) {
      return false;
    }
    if (elapsedTime != null ? !elapsedTime.equals(flight.elapsedTime) : flight.elapsedTime != null) {
      return false;
    }
    return delayed.equals(flight.delayed);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = dofM;
    result = 31 * result + carrier.hashCode();
    result = 31 * result + dofW;
    result = 31 * result + (tailNum != null ? tailNum.hashCode() : 0);
    result = 31 * result + flightNum;
    result = 31 * result + (originID != null ? originID.hashCode() : 0);
    result = 31 * result + origin.hashCode();
    result = 31 * result + (destId != null ? destId.hashCode() : 0);
    result = 31 * result + dest.hashCode();
    temp = Double.doubleToLongBits(scheduleDepTime);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + (deptime != null ? deptime.hashCode() : 0);
    result = 31 * result + depDelayMins.hashCode();
    result = 31 * result + (scheduledArrTime != null ? scheduledArrTime.hashCode() : 0);
    result = 31 * result + (arrTime != null ? arrTime.hashCode() : 0);
    result = 31 * result + (arrDelay != null ? arrDelay.hashCode() : 0);
    result = 31 * result + (elapsedTime != null ? elapsedTime.hashCode() : 0);
    result = 31 * result + distance;
    result = 31 * result + delayed.hashCode();
    return result;
  }

  public StructuredRecord toStructuredRecord() {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set("dofM", dofM);
    builder.set("dofW", dofW);
    builder.set("carrier", carrier);
    builder.set("tailNum", tailNum);
    builder.set("flightNum", flightNum);
    builder.set("originID", originID);
    builder.set("origin", origin);
    builder.set("destId", destId);
    builder.set("dest", dest);
    builder.set("scheduleDepTime", scheduleDepTime);
    builder.set("deptime", deptime);
    builder.set("depDelayMins", depDelayMins);
    builder.set("scheduledArrTime", scheduledArrTime);
    builder.set("arrTime", arrTime);
    builder.set("arrDelay", arrDelay);
    builder.set("elapsedTime", elapsedTime);
    builder.set("distance", distance);
    if (delayed != null) {
      builder.set("delayed", delayed);
    }
    return builder.build();
  }
}

