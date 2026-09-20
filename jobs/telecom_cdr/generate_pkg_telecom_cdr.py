#!/usr/bin/env python3
"""Generates pkg_telecom_cdr.dtsx -- a real, converter-targeted SSIS package,
built entirely from scratch, no dependency on any other .dtsx file.

Built with the exact same helper-function shape as the reference repo's
source/ssis_packages/generate_pkg_orders_synthetic.py (component builders
that return a raw XML string, wired together by hand-listed <path> elements)
-- proof that the same authoring pattern works unmodified for a completely
different domain. Nothing here or in migrator/ hardcodes a telecom word; the
converter discovers every column/table name from this file's own XML.

Business shape: ingest one landing file of Call Detail Records (CDRs),
validate against the real reference tables (subscriber, cell tower, plan),
compute call cost, load clean calls into fact_calls and a per-subscriber
rollup into subscriber_daily_usage, quarantine everything else with a
reason (UNKNOWN_SUBSCRIBER, UNKNOWN_TOWER, UNKNOWN_PLAN, BAD_DURATION).

.dtsx files are never hand-edited: edit this script and re-run it.
"""
from __future__ import annotations

import pathlib

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "pkg_telecom_cdr.dtsx"

DF = r"Package\CDR Batch Loop\Ingest CDRs"
SRC_OUT = f"{DF}\\Extract CDR Records.Outputs[Flat File Source Output]"


def source_col(name: str, dtype: str) -> str:
    return (f'                    <outputColumn refId="{SRC_OUT}.Columns[{name}]" name="{name}" '
            f'dataType="{dtype}" lineageId="{SRC_OUT}.Columns[{name}]" />\n')


def lookup_component(name: str, sql_table: str, key_col: str, key_lineage: str,
                      cache_type: str, match_cols: list[tuple[str, str]]) -> str:
    """A Microsoft.Lookup component, joining key_col (found via key_lineage)
    against sql_table."""
    match_outputs = "".join(
        f'                    <outputColumn refId="{DF}\\{name}.Outputs[Lookup Match Output].Columns[{c}]" '
        f'name="{c}" dataType="{dt}" lineageId="{DF}\\{name}.Outputs[Lookup Match Output].Columns[{c}]">\n'
        f'                      <properties>\n'
        f'                        <property name="CopyFromReferenceColumn" dataType="System.String">{c}</property>\n'
        f'                      </properties>\n'
        f'                    </outputColumn>\n'
        for c, dt in match_cols
    )
    return f'''            <component refId="{DF}\\{name}" componentClassID="Microsoft.Lookup" name="{name}" usesDispositions="true" version="1">
              <properties>
                <property name="SqlCommand" dataType="System.String">select * from (select * from {sql_table}) as refTable</property>
                <property name="SqlCommandParam" dataType="System.String">select * from (select * from {sql_table}) as refTable</property>
                <property name="ConnectionType" dataType="System.String">0</property>
                <property name="CacheType" dataType="System.String">{cache_type}</property>
                <property name="NoMatchBehavior" dataType="System.String">0</property>
                <property name="NoMatchCachePercentage" dataType="System.String">0</property>
                <property name="MaxMemoryUsage" dataType="System.String">25</property>
                <property name="MaxMemoryUsage64" dataType="System.String">25</property>
                <property name="DefaultCodePage" dataType="System.String">1252</property>
                <property name="TreatDuplicateKeysAsError" dataType="System.String">false</property>
              </properties>
              <connections>
                <connection refId="{DF}\\{name}.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Telecom Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\{name}.Inputs[Lookup Input]" name="Lookup Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[Lookup Input].Columns[{key_col}]" cachedName="{key_col}" cachedDataType="wstr" lineageId="{key_lineage}">
                      <properties>
                        <property name="JoinToReferenceColumn" dataType="System.String">{key_col}</property>
                        <property name="CopyFromReferenceColumn" dataType="System.String" />
                      </properties>
                    </inputColumn>
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\{name}.Outputs[Lookup Match Output]" name="Lookup Match Output">
                  <outputColumns>
{match_outputs}                  </outputColumns>
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\{name}.Outputs[Lookup No Match Output]" name="Lookup No Match Output">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\{name}.Outputs[Lookup Error Output]" name="Lookup Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''


def tag_component(name: str, reason: str, key_col: str, key_lineage: str) -> str:
    """A Microsoft.DerivedColumn that stamps a literal `reason` string."""
    return f'''            <component refId="{DF}\\{name}" componentClassID="Microsoft.DerivedColumn" name="{name}" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\{name}.Inputs[Derived Column Input]" name="Derived Column Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[Derived Column Input].Columns[{key_col}]" cachedName="{key_col}" cachedDataType="wstr" lineageId="{key_lineage}" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\{name}.Outputs[Derived Column Output]" name="Derived Column Output">
                  <outputColumns>
                    <outputColumn refId="{DF}\\{name}.Outputs[Derived Column Output].Columns[reason]" name="reason" dataType="wstr" lineageId="{DF}\\{name}.Outputs[Derived Column Output].Columns[reason]">
                      <properties>
                        <property name="FriendlyExpression" dataType="System.String">"{reason}"</property>
                      </properties>
                    </outputColumn>
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''


def reject_component(name: str, reason_src: str) -> str:
    """A Microsoft.OLEDBDestination into quarantine_cdr, fed by call_id (from
    the source) and reason (from the matching Tag component)."""
    return f'''            <component refId="{DF}\\{name}" componentClassID="Microsoft.OLEDBDestination" name="{name}" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[quarantine_cdr]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\{name}.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Telecom Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\{name}.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[OLE DB Destination Input].Columns[call_id]" cachedName="call_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[call_id]" />
                    <inputColumn refId="{DF}\\{name}.Inputs[OLE DB Destination Input].Columns[reason]" cachedName="reason" cachedDataType="wstr" lineageId="{reason_src}" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\{name}.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''


def main() -> None:
    source_cols = "".join(
        source_col(n, dt) for n, dt in [
            ("call_id", "wstr"), ("subscriber_id", "wstr"), ("tower_id", "wstr"),
            ("plan_code", "wstr"), ("call_type", "wstr"), ("call_ts", "i8"),
            ("duration_sec", "i4"),
        ]
    )

    extract = f'''            <component refId="{DF}\\Extract CDR Records" componentClassID="Microsoft.FlatFileSource" name="Extract CDR Records" usesDispositions="true" version="1">
              <properties />
              <connections>
                <connection refId="{DF}\\Extract CDR Records.Connections[FlatFileConnection]" connectionManagerRefId="Package.ConnectionManagers[Telecom Landing]" name="FlatFileConnection" />
              </connections>
              <outputs>
                <output refId="{SRC_OUT}" name="Flat File Source Output">
                  <outputColumns>
{source_cols}                  </outputColumns>
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\Extract CDR Records.Outputs[Flat File Source Error Output]" name="Flat File Source Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    lookup_subscriber = lookup_component(
        "Lookup Subscriber", "dim_subscriber", "subscriber_id",
        f"{SRC_OUT}.Columns[subscriber_id]", "0",
        [("status", "wstr")],
    )
    lookup_tower = lookup_component(
        "Lookup Cell Tower", "dim_cell_tower", "tower_id",
        f"{SRC_OUT}.Columns[tower_id]", "1",
        [("location", "wstr")],
    )
    lookup_plan = lookup_component(
        "Lookup Plan", "dim_plan", "plan_code",
        f"{SRC_OUT}.Columns[plan_code]", "1",
        [("rate_per_min", "r8"), ("currency", "wstr")],
    )

    validate_duration = f'''            <component refId="{DF}\\Validate Duration" componentClassID="Microsoft.ConditionalSplit" name="Validate Duration" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\Validate Duration.Inputs[Conditional Split Input]" name="Conditional Split Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Validate Duration.Inputs[Conditional Split Input].Columns[duration_sec]" cachedName="duration_sec" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[duration_sec]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Validate Duration.Outputs[bad_duration]" name="bad_duration">
                  <properties>
                    <property name="FriendlyExpression" dataType="System.String">!(duration_sec &gt;= 0 &amp;&amp; duration_sec &lt;= 7200)</property>
                    <property name="Order" dataType="System.String">0</property>
                  </properties>
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\Validate Duration.Outputs[clean]" name="clean">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    compute_cost = f'''            <component refId="{DF}\\Compute Cost" componentClassID="Microsoft.DerivedColumn" name="Compute Cost" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\Compute Cost.Inputs[Derived Column Input]" name="Derived Column Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Compute Cost.Inputs[Derived Column Input].Columns[duration_sec]" cachedName="duration_sec" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[duration_sec]" />
                    <inputColumn refId="{DF}\\Compute Cost.Inputs[Derived Column Input].Columns[rate_per_min]" cachedName="rate_per_min" cachedDataType="r8" lineageId="{DF}\\Lookup Plan.Outputs[Lookup Match Output].Columns[rate_per_min]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Compute Cost.Outputs[Derived Column Output]" name="Derived Column Output">
                  <outputColumns>
                    <outputColumn refId="{DF}\\Compute Cost.Outputs[Derived Column Output].Columns[cost]" name="cost" dataType="r8" lineageId="{DF}\\Compute Cost.Outputs[Derived Column Output].Columns[cost]">
                      <properties>
                        <property name="FriendlyExpression" dataType="System.String">(duration_sec / 60.0) * rate_per_min</property>
                      </properties>
                    </outputColumn>
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    load_fact_calls = f'''            <component refId="{DF}\\Load Fact Calls" componentClassID="Microsoft.OLEDBDestination" name="Load Fact Calls" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[fact_calls]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\Load Fact Calls.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Telecom Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[call_id]" cachedName="call_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[call_id]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[subscriber_id]" cachedName="subscriber_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[subscriber_id]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[tower_id]" cachedName="tower_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[tower_id]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[plan_code]" cachedName="plan_code" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[plan_code]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[call_type]" cachedName="call_type" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[call_type]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[call_ts]" cachedName="call_ts" cachedDataType="i8" lineageId="{SRC_OUT}.Columns[call_ts]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[duration_sec]" cachedName="duration_sec" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[duration_sec]" />
                    <inputColumn refId="{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input].Columns[cost]" cachedName="cost" cachedDataType="r8" lineageId="{DF}\\Compute Cost.Outputs[Derived Column Output].Columns[cost]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Load Fact Calls.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    aggregate = f'''            <component refId="{DF}\\Aggregate Subscriber Usage" componentClassID="Microsoft.Aggregate" name="Aggregate Subscriber Usage" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input]" name="Aggregate Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input].Columns[call_id]" cachedName="call_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[call_id]" />
                    <inputColumn refId="{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input].Columns[subscriber_id]" cachedName="subscriber_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[subscriber_id]" />
                    <inputColumn refId="{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input].Columns[duration_sec]" cachedName="duration_sec" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[duration_sec]" />
                    <inputColumn refId="{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input].Columns[cost]" cachedName="cost" cachedDataType="r8" lineageId="{DF}\\Compute Cost.Outputs[Derived Column Output].Columns[cost]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1]" name="Aggregate Output 1">
                  <outputColumns>
                    <outputColumn refId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[subscriber_id]" name="subscriber_id" dataType="wstr" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[subscriber_id]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">GroupBy</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[call_count]" name="call_count" dataType="i4" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[call_count]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Count</property>
                        <property name="SourceColumn" dataType="System.String">call_id</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_duration_sec]" name="total_duration_sec" dataType="i4" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_duration_sec]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Sum</property>
                        <property name="SourceColumn" dataType="System.String">duration_sec</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_cost]" name="total_cost" dataType="r8" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_cost]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Sum</property>
                        <property name="SourceColumn" dataType="System.String">cost</property>
                      </properties>
                    </outputColumn>
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    load_usage = f'''            <component refId="{DF}\\Load Subscriber Daily Usage" componentClassID="Microsoft.OLEDBDestination" name="Load Subscriber Daily Usage" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[subscriber_daily_usage]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\Load Subscriber Daily Usage.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Telecom Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input].Columns[subscriber_id]" cachedName="subscriber_id" cachedDataType="wstr" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[subscriber_id]" />
                    <inputColumn refId="{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input].Columns[call_count]" cachedName="call_count" cachedDataType="i4" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[call_count]" />
                    <inputColumn refId="{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input].Columns[total_duration_sec]" cachedName="total_duration_sec" cachedDataType="i4" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_duration_sec]" />
                    <inputColumn refId="{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input].Columns[total_cost]" cachedName="total_cost" cachedDataType="r8" lineageId="{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1].Columns[total_cost]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Load Subscriber Daily Usage.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    components = (
        extract + lookup_subscriber +
        tag_component("Tag Unknown Subscriber", "UNKNOWN_SUBSCRIBER",
                       "call_id", f"{SRC_OUT}.Columns[call_id]") +
        reject_component("Reject Unknown Subscriber",
                          f"{DF}\\Tag Unknown Subscriber.Outputs[Derived Column Output].Columns[reason]") +
        lookup_tower +
        tag_component("Tag Unknown Tower", "UNKNOWN_TOWER",
                       "call_id", f"{SRC_OUT}.Columns[call_id]") +
        reject_component("Reject Unknown Tower",
                          f"{DF}\\Tag Unknown Tower.Outputs[Derived Column Output].Columns[reason]") +
        lookup_plan +
        tag_component("Tag Unknown Plan", "UNKNOWN_PLAN",
                       "call_id", f"{SRC_OUT}.Columns[call_id]") +
        reject_component("Reject Unknown Plan",
                          f"{DF}\\Tag Unknown Plan.Outputs[Derived Column Output].Columns[reason]") +
        validate_duration +
        tag_component("Tag Bad Duration", "BAD_DURATION",
                       "call_id", f"{SRC_OUT}.Columns[call_id]") +
        reject_component("Reject Bad Duration",
                          f"{DF}\\Tag Bad Duration.Outputs[Derived Column Output].Columns[reason]") +
        compute_cost + load_fact_calls + aggregate + load_usage
    )

    def path(name: str, start: str, end: str) -> str:
        return (f'            <path refId="{DF}.Paths[{name}]" name="{name}" '
                f'startId="{start}" endId="{end}" />\n')

    paths = (
        path("Flat File Source Output -&gt; Lookup Subscriber",
             f"{DF}\\Extract CDR Records.Outputs[Flat File Source Output]",
             f"{DF}\\Lookup Subscriber.Inputs[Lookup Input]") +
        path("Lookup No Match Output -&gt; Tag Unknown Subscriber",
             f"{DF}\\Lookup Subscriber.Outputs[Lookup No Match Output]",
             f"{DF}\\Tag Unknown Subscriber.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Unknown Subscriber",
             f"{DF}\\Tag Unknown Subscriber.Outputs[Derived Column Output]",
             f"{DF}\\Reject Unknown Subscriber.Inputs[OLE DB Destination Input]") +
        path("Lookup Match Output -&gt; Lookup Cell Tower",
             f"{DF}\\Lookup Subscriber.Outputs[Lookup Match Output]",
             f"{DF}\\Lookup Cell Tower.Inputs[Lookup Input]") +
        path("Lookup No Match Output -&gt; Tag Unknown Tower",
             f"{DF}\\Lookup Cell Tower.Outputs[Lookup No Match Output]",
             f"{DF}\\Tag Unknown Tower.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Unknown Tower",
             f"{DF}\\Tag Unknown Tower.Outputs[Derived Column Output]",
             f"{DF}\\Reject Unknown Tower.Inputs[OLE DB Destination Input]") +
        path("Lookup Match Output -&gt; Lookup Plan",
             f"{DF}\\Lookup Cell Tower.Outputs[Lookup Match Output]",
             f"{DF}\\Lookup Plan.Inputs[Lookup Input]") +
        path("Lookup No Match Output -&gt; Tag Unknown Plan",
             f"{DF}\\Lookup Plan.Outputs[Lookup No Match Output]",
             f"{DF}\\Tag Unknown Plan.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Unknown Plan",
             f"{DF}\\Tag Unknown Plan.Outputs[Derived Column Output]",
             f"{DF}\\Reject Unknown Plan.Inputs[OLE DB Destination Input]") +
        path("Lookup Match Output -&gt; Validate Duration",
             f"{DF}\\Lookup Plan.Outputs[Lookup Match Output]",
             f"{DF}\\Validate Duration.Inputs[Conditional Split Input]") +
        path("bad_duration -&gt; Tag Bad Duration",
             f"{DF}\\Validate Duration.Outputs[bad_duration]",
             f"{DF}\\Tag Bad Duration.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Bad Duration",
             f"{DF}\\Tag Bad Duration.Outputs[Derived Column Output]",
             f"{DF}\\Reject Bad Duration.Inputs[OLE DB Destination Input]") +
        path("clean -&gt; Compute Cost",
             f"{DF}\\Validate Duration.Outputs[clean]",
             f"{DF}\\Compute Cost.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Load Fact Calls",
             f"{DF}\\Compute Cost.Outputs[Derived Column Output]",
             f"{DF}\\Load Fact Calls.Inputs[OLE DB Destination Input]") +
        path("Derived Column Output -&gt; Aggregate Subscriber Usage",
             f"{DF}\\Compute Cost.Outputs[Derived Column Output]",
             f"{DF}\\Aggregate Subscriber Usage.Inputs[Aggregate Input]") +
        path("Aggregate Output 1 -&gt; Load Subscriber Daily Usage",
             f"{DF}\\Aggregate Subscriber Usage.Outputs[Aggregate Output 1]",
             f"{DF}\\Load Subscriber Daily Usage.Inputs[OLE DB Destination Input]")
    )

    text = f'''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts" DTS:refId="Package" DTS:CreationName="Microsoft.Package" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000001}}" DTS:ExecutableType="Microsoft.Package" DTS:LocaleID="1033" DTS:ObjectName="Telecom CDR" DTS:PackageType="5" DTS:VersionBuild="1" DTS:VersionGUID="{{C7DE0002-0000-0000-0000-000000000002}}">
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[Telecom Landing]" DTS:CreationName="FLATFILE" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000010}}" DTS:ObjectName="Telecom Landing">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="/opt/nifi/data/landing/*.ndjson" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[Telecom Warehouse]" DTS:CreationName="OLEDB" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000011}}" DTS:ObjectName="Telecom Warehouse">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="Data Source=localhost;Initial Catalog=TelecomDW;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  <DTS:Variables />
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\CDR Batch Loop" DTS:CreationName="STOCK:FOREACHLOOP" DTS:Description="Loop for each landing file" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000030}}" DTS:ExecutableType="STOCK:FOREACHLOOP" DTS:LocaleID="1033" DTS:ObjectName="CDR Batch Loop">
      <DTS:ForEachEnumerator DTS:CreationName="Microsoft.ForEachFileEnumerator" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000031}}" DTS:ObjectName="Foreach File Enumerator">
        <DTS:ObjectData>
          <ForEachFileEnumeratorProperties>
            <FEFEProperty Folder="/opt/nifi/data/landing" />
            <FEFEProperty FileSpec="*.ndjson" />
          </ForEachFileEnumeratorProperties>
        </DTS:ObjectData>
      </DTS:ForEachEnumerator>
      <DTS:Variables />
      <DTS:Executables>
    <DTS:Executable DTS:refId="{DF}" DTS:CreationName="Microsoft.Pipeline" DTS:Description="Data Flow Task" DTS:DTSID="{{C7DE0002-0000-0000-0000-000000000020}}" DTS:ExecutableType="Microsoft.Pipeline" DTS:LocaleID="1033" DTS:ObjectName="Ingest CDRs">
      <DTS:Variables />
      <DTS:ObjectData>
        <pipeline version="1">
          <components>
{components}          </components>
          <paths>
{paths}          </paths>
        </pipeline>
      </DTS:ObjectData>
    </DTS:Executable>
      </DTS:Executables>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>
'''

    OUT.write_text(text)
    print(f"wrote {OUT} ({len(text.splitlines())} lines)")


if __name__ == "__main__":
    main()
