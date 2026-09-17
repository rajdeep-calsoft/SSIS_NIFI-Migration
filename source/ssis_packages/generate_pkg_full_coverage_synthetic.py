#!/usr/bin/env python3
"""Generates pkg_full_coverage_synthetic.dtsx from pkg_orders_etl.dtsx.

BEST-EFFORT SYNTHETIC COVERAGE PACKAGE -- built to exercise, in one file,
every reject reason, every diagnostic warning, and all three non-1:1 SSIS
component narrowings (Sort/Aggregate/Script Component) the converter
currently supports, so it can be run through the control panel UI and
verified end to end.

Built by taking pkg_orders_etl.dtsx -- a real, proven, 100%-converting
package (17/17 components, verified live against NiFi, verified at 50k rows
against an independent oracle) -- as the base, and adding on top of it:

  1. A STOCK:FOREACHLOOP container wrapping the Data Flow Task
     -> CONTROL_FLOW_CONTAINER
  2. Lookup Product: CacheType=1 -> LOOKUP_PARTIAL_CACHE
                      a filtered SqlCommand -> LOOKUP_FILTERED_REFERENCE
  3. A Sort component (EliminateDuplicates) inserted between "Compute Order
     Line Id" and "Check Replay" -- the same two-stage dedup shape
     destination/generator/gen/build_flow.py's real flow uses (within-file
     Sort dedup, THEN cross-file Lookup replay check) -> SORT_DEDUP_ONLY
  4. An Aggregate component branching off Business Rules' "clean" output in
     parallel with Load Order Items, rolling up into the REAL `orders`
     table (order_id, customer_id, order_total, item_count) -- the same
     shape build_flow.py's own "8. Aggregate orders" does -> AGGREGATE_SIMPLE_GROUPBY
  5. A Script Component fed from Lookup Product's Lookup Error Output,
     using the one recognised GetErrorDescription idiom, writing a 6th,
     EXTRA quarantine reason (LOOKUP_ERROR -- outside the 5-reason official
     taxonomy, added specifically to exercise this feature) -> SCRIPT_ERROR_CATALOGUE_PARTIAL
  6. Business Rules' pre-existing "clean" default branch already gives
     CONDITIONALSPLIT_DEFAULT_DERIVED for free -- untouched.

NOT included, and why: LOOKUP_DATE_KEY_CAST (needs a date-typed lookup key
against a real target column; not worth the schema risk for a demo file)
and LOOKUP_COMPOSITE_KEY (its severity is ERROR, not WARN -- including it
would make that Lookup refused, defeating "see it convert at 100%").
FLATFILE_ROW_DELIMITER_INFERRED only fires for a column-delimited FLATFILE
connection manager (see corpus/packages/L4.dtsx); this package's landing
files are NDJSON like the rest of this project's real packages, so it does
not apply here without breaking that consistency.

.dtsx files are never hand-edited (CLAUDE.md): edit this script and re-run
it, the same relationship every other generate_*.py has to its .dtsx.
"""
from __future__ import annotations

import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
BASE = (HERE / "pkg_orders_etl.dtsx").read_text()
OUT = HERE / "pkg_full_coverage_synthetic.dtsx"

PKG_NAME_OLD = "Orders ETL"
PKG_NAME_NEW = "Full Coverage Synthetic"
DF_OLD = r"Package\Ingest Orders"
DF_NEW = r"Package\Batch Loop\Ingest Orders"


def main() -> None:
    text = BASE

    # -- rename the package and rescope every refId under the new loop ----
    text = text.replace(f'DTS:ObjectName="{PKG_NAME_OLD}"', f'DTS:ObjectName="{PKG_NAME_NEW}"', 1)
    text = text.replace(re.escape(DF_OLD).replace(r"\\", "\\"), DF_NEW)
    # the .replace above with escape is fragile with backslashes -- do it literally instead
    text = BASE.replace(f'DTS:ObjectName="{PKG_NAME_OLD}"', f'DTS:ObjectName="{PKG_NAME_NEW}"', 1)
    text = text.replace(DF_OLD, DF_NEW)

    # -- 1. Lookup Product: trigger LOOKUP_PARTIAL_CACHE + LOOKUP_FILTERED_REFERENCE,
    #       and give its error output a real ErrorCode column (the convention
    #       proven in corpus/packages/L4.dtsx's own Lookup error output) -----
    text = text.replace(
        '<property name="SqlCommand" dataType="System.String">select * from (select * from products) as refTable</property>\n'
        '                <property name="SqlCommandParam" dataType="System.String">select * from (select * from products) as refTable</property>\n'
        '                <property name="ConnectionType" dataType="System.String">0</property>\n'
        '                <property name="CacheType" dataType="System.String">0</property>',
        '<property name="SqlCommand" dataType="System.String">select * from (select * from products where unit_price &gt; 0) as refTable</property>\n'
        '                <property name="SqlCommandParam" dataType="System.String">select * from (select * from products where unit_price &gt; 0) as refTable</property>\n'
        '                <property name="ConnectionType" dataType="System.String">0</property>\n'
        '                <property name="CacheType" dataType="System.String">1</property>',
        1,
    )
    text = text.replace(
        f'<output refId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output]" name="Lookup Error Output" isErrorOut="true">\n'
        '                  <outputColumns />\n'
        '                  <externalMetadataColumns />\n'
        '                </output>',
        f'<output refId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output]" name="Lookup Error Output" isErrorOut="true">\n'
        '                  <outputColumns>\n'
        f'                    <outputColumn refId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output].Columns[ErrorCode]" name="ErrorCode" dataType="i4" lineageId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output].Columns[ErrorCode]" />\n'
        '                  </outputColumns>\n'
        '                  <externalMetadataColumns />\n'
        '                </output>',
        1,
    )

    # -- 2. insert Sort Dedup Lines between Compute Order Line Id and Check Replay --
    sort_component = f'''            <component refId="{DF_NEW}\\Sort Dedup Lines" componentClassID="Microsoft.Sort" name="Sort Dedup Lines" usesDispositions="true" version="1">
              <properties>
                <property name="EliminateDuplicates" dataType="System.Boolean">true</property>
              </properties>
              <inputs>
                <input refId="{DF_NEW}\\Sort Dedup Lines.Inputs[Sort Input]" name="Sort Input">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Sort Dedup Lines.Inputs[Sort Input].Columns[order_line_id]" cachedName="order_line_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Compute Order Line Id.Outputs[Derived Column Output].Columns[order_line_id]">
                      <properties>
                        <property name="sortKeyPosition" dataType="System.Int32">1</property>
                      </properties>
                    </inputColumn>
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Sort Dedup Lines.Outputs[Sort Output]" name="Sort Output">
                  <outputColumns>
                    <outputColumn refId="{DF_NEW}\\Sort Dedup Lines.Outputs[Sort Output].Columns[order_line_id]" name="order_line_id" dataType="wstr" lineageId="{DF_NEW}\\Sort Dedup Lines.Outputs[Sort Output].Columns[order_line_id]" />
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''
    text = text.replace(
        f'            <component refId="{DF_NEW}\\Check Replay"',
        sort_component + f'            <component refId="{DF_NEW}\\Check Replay"',
        1,
    )
    text = text.replace(
        f'<path refId="{DF_NEW}.Paths[Derived Column Output -&gt; Check Replay]" name="Derived Column Output -&gt; Check Replay" startId="{DF_NEW}\\Compute Order Line Id.Outputs[Derived Column Output]" endId="{DF_NEW}\\Check Replay.Inputs[Lookup Input]" />',
        f'<path refId="{DF_NEW}.Paths[Derived Column Output -&gt; Sort Dedup Lines]" name="Derived Column Output -&gt; Sort Dedup Lines" startId="{DF_NEW}\\Compute Order Line Id.Outputs[Derived Column Output]" endId="{DF_NEW}\\Sort Dedup Lines.Inputs[Sort Input]" />\n'
        f'            <path refId="{DF_NEW}.Paths[Sort Output -&gt; Check Replay]" name="Sort Output -&gt; Check Replay" startId="{DF_NEW}\\Sort Dedup Lines.Outputs[Sort Output]" endId="{DF_NEW}\\Check Replay.Inputs[Lookup Input]" />',
        1,
    )

    # -- 3. Aggregate Order Rollup, branching off Business Rules' clean output,
    #       in parallel with Load Order Items, writing into the real `orders` table --
    aggregate_component = f'''            <component refId="{DF_NEW}\\Aggregate Order Rollup" componentClassID="Microsoft.Aggregate" name="Aggregate Order Rollup" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input]" name="Aggregate Input">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[order_id]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[customer_id]" cachedName="customer_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[customer_id]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[order_ts]" cachedName="order_ts" cachedDataType="i8" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[order_ts]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[status]" cachedName="status" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[status]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[channel]" cachedName="channel" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[channel]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[currency]" cachedName="currency" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[currency]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[qty]" cachedName="qty" cachedDataType="i4" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[qty]" />
                    <inputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[line_total]" cachedName="line_total" cachedDataType="r8" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[line_total]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1]" name="Aggregate Output 1">
                  <outputColumns>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]" name="order_id" dataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">GroupBy</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]" name="customer_id" dataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">customer_id</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]" name="order_ts" dataType="i8" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">order_ts</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]" name="status" dataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">status</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]" name="channel" dataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">channel</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]" name="currency" dataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">currency</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]" name="order_total" dataType="r8" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Sum</property>
                        <property name="SourceColumn" dataType="System.String">line_total</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]" name="item_count" dataType="i4" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Sum</property>
                        <property name="SourceColumn" dataType="System.String">qty</property>
                      </properties>
                    </outputColumn>
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
            <component refId="{DF_NEW}\\Load Orders Rollup" componentClassID="Microsoft.OLEDBDestination" name="Load Orders Rollup" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[orders]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF_NEW}\\Load Orders Rollup.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[customer_id]" cachedName="customer_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_ts]" cachedName="order_ts" cachedDataType="i8" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[status]" cachedName="status" cachedDataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[channel]" cachedName="channel" cachedDataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[currency]" cachedName="currency" cachedDataType="wstr" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_total]" cachedName="order_total" cachedDataType="r8" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]" />
                    <inputColumn refId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[item_count]" cachedName="item_count" cachedDataType="i4" lineageId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Load Orders Rollup.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''
    text = text.replace(
        f'            <component refId="{DF_NEW}\\Load Order Items"',
        aggregate_component + f'            <component refId="{DF_NEW}\\Load Order Items"',
        1,
    )

    # -- 4. Script Component "Get Error Description", fed from Lookup Product's
    #       Lookup Error Output, writing an EXTRA quarantine reason ----------
    script_source = (
        "public class ScriptMain\n"
        "{\n"
        "    public override void Input0_ProcessInputRow(Input0Buffer Row)\n"
        "    {\n"
        "        Row.ErrorDescription = this.ComponentMetaData.GetErrorDescription(Row.ErrorCode);\n"
        "    }\n"
        "}\n"
    )
    script_component = f'''            <component refId="{DF_NEW}\\Get Error Description" componentClassID="Microsoft.ManagedComponentHost" name="Get Error Description" usesDispositions="true" version="1">
              <properties>
                <property dataType="System.String" description="Stores the source code of the component" isArray="true" name="SourceCode" state="cdata">
                  <arrayElements arrayElementCount="3">
                    <arrayElement dataType="System.String"><![CDATA[main.cs]]></arrayElement>
                    <arrayElement dataType="System.String"><![CDATA[UTF8]]></arrayElement>
                    <arrayElement dataType="System.String"><![CDATA[{script_source}]]></arrayElement>
                  </arrayElements>
                </property>
              </properties>
              <inputs>
                <input refId="{DF_NEW}\\Get Error Description.Inputs[Input 0]" name="Input 0">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Get Error Description.Inputs[Input 0].Columns[ErrorCode]" cachedName="ErrorCode" cachedDataType="i4" lineageId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output].Columns[ErrorCode]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Get Error Description.Outputs[Output 0]" name="Output 0">
                  <outputColumns>
                    <outputColumn refId="{DF_NEW}\\Get Error Description.Outputs[Output 0].Columns[ErrorDescription]" name="ErrorDescription" dataType="wstr" lineageId="{DF_NEW}\\Get Error Description.Outputs[Output 0].Columns[ErrorDescription]" />
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
            <component refId="{DF_NEW}\\Tag Lookup Error" componentClassID="Microsoft.DerivedColumn" name="Tag Lookup Error" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF_NEW}\\Tag Lookup Error.Inputs[Derived Column Input]" name="Derived Column Input">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Tag Lookup Error.Inputs[Derived Column Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[order_id]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Tag Lookup Error.Outputs[Derived Column Output]" name="Derived Column Output">
                  <outputColumns>
                    <outputColumn refId="{DF_NEW}\\Tag Lookup Error.Outputs[Derived Column Output].Columns[reason]" name="reason" dataType="wstr" lineageId="{DF_NEW}\\Tag Lookup Error.Outputs[Derived Column Output].Columns[reason]">
                      <properties>
                        <property name="FriendlyExpression" dataType="System.String">"LOOKUP_ERROR"</property>
                      </properties>
                    </outputColumn>
                  </outputColumns>
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
            <component refId="{DF_NEW}\\Reject Lookup Error" componentClassID="Microsoft.OLEDBDestination" name="Reject Lookup Error" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[quarantine_records]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF_NEW}\\Reject Lookup Error.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF_NEW}\\Reject Lookup Error.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF_NEW}\\Reject Lookup Error.Inputs[OLE DB Destination Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{DF_NEW}\\Extract Order Lines.Outputs[Flat File Source Output].Columns[order_id]" />
                    <inputColumn refId="{DF_NEW}\\Reject Lookup Error.Inputs[OLE DB Destination Input].Columns[reason]" cachedName="reason" cachedDataType="wstr" lineageId="{DF_NEW}\\Tag Lookup Error.Outputs[Derived Column Output].Columns[reason]" />
                    <inputColumn refId="{DF_NEW}\\Reject Lookup Error.Inputs[OLE DB Destination Input].Columns[detail]" cachedName="detail" cachedDataType="wstr" lineageId="{DF_NEW}\\Get Error Description.Outputs[Output 0].Columns[ErrorDescription]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF_NEW}\\Reject Lookup Error.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''
    text = text.replace(
        f'            <component refId="{DF_NEW}\\Lookup Customer"',
        script_component + f'            <component refId="{DF_NEW}\\Lookup Customer"',
        1,
    )

    # -- paths: wire the two new branches (Aggregate, Script) --------------
    new_paths = (
        f'            <path refId="{DF_NEW}.Paths[clean -&gt; Aggregate Order Rollup]" name="clean -&gt; Aggregate Order Rollup" startId="{DF_NEW}\\Business Rules.Outputs[clean]" endId="{DF_NEW}\\Aggregate Order Rollup.Inputs[Aggregate Input]" />\n'
        f'            <path refId="{DF_NEW}.Paths[Aggregate Output 1 -&gt; Load Orders Rollup]" name="Aggregate Output 1 -&gt; Load Orders Rollup" startId="{DF_NEW}\\Aggregate Order Rollup.Outputs[Aggregate Output 1]" endId="{DF_NEW}\\Load Orders Rollup.Inputs[OLE DB Destination Input]" />\n'
        f'            <path refId="{DF_NEW}.Paths[Lookup Error Output -&gt; Get Error Description]" name="Lookup Error Output -&gt; Get Error Description" startId="{DF_NEW}\\Lookup Product.Outputs[Lookup Error Output]" endId="{DF_NEW}\\Get Error Description.Inputs[Input 0]" />\n'
        f'            <path refId="{DF_NEW}.Paths[Output 0 -&gt; Tag Lookup Error]" name="Output 0 -&gt; Tag Lookup Error" startId="{DF_NEW}\\Get Error Description.Outputs[Output 0]" endId="{DF_NEW}\\Tag Lookup Error.Inputs[Derived Column Input]" />\n'
        f'            <path refId="{DF_NEW}.Paths[Derived Column Output -&gt; Reject Lookup Error]" name="Derived Column Output -&gt; Reject Lookup Error" startId="{DF_NEW}\\Tag Lookup Error.Outputs[Derived Column Output]" endId="{DF_NEW}\\Reject Lookup Error.Inputs[OLE DB Destination Input]" />\n'
    )
    text = text.replace("          </paths>", new_paths + "          </paths>", 1)

    # -- wrap the Data Flow Task's own DTS:Executable in a STOCK:FOREACHLOOP --
    pipeline_open = (
        f'    <DTS:Executable DTS:refId="{DF_NEW}" DTS:CreationName="Microsoft.Pipeline" '
        'DTS:Description="Data Flow Task" DTS:DTSID="{B3D5B001-0000-0000-0000-000000000020}" '
        'DTS:ExecutableType="Microsoft.Pipeline" DTS:LocaleID="1033" DTS:ObjectName="Ingest Orders">'
    )
    loop_open = (
        '    <DTS:Executable DTS:refId="Package\\Batch Loop" DTS:CreationName="STOCK:FOREACHLOOP" '
        'DTS:Description="Loop for each landing file" DTS:DTSID="{B3D5B001-0000-0000-0000-000000000030}" '
        'DTS:ExecutableType="STOCK:FOREACHLOOP" DTS:LocaleID="1033" DTS:ObjectName="Batch Loop">\n'
        '      <DTS:ForEachEnumerator DTS:CreationName="Microsoft.ForEachFileEnumerator" '
        'DTS:DTSID="{B3D5B001-0000-0000-0000-000000000031}" DTS:ObjectName="Foreach File Enumerator">\n'
        '        <DTS:ObjectData>\n'
        '          <ForEachFileEnumeratorProperties>\n'
        '            <FEFEProperty Folder="/opt/nifi/data/landing" />\n'
        '            <FEFEProperty FileSpec="*.ndjson" />\n'
        '          </ForEachFileEnumeratorProperties>\n'
        '        </DTS:ObjectData>\n'
        '      </DTS:ForEachEnumerator>\n'
        '      <DTS:Variables />\n'
        '      <DTS:Executables>\n'
        + pipeline_open
    )
    assert pipeline_open in text, "pipeline open tag not found verbatim -- base file structure changed"
    text = text.replace(pipeline_open, loop_open, 1)

    # close the wrapping loop's <DTS:Executables></DTS:Executable> after the
    # pipeline task's own closing tags, right before the outer </DTS:Executables>
    old_close = "    </DTS:Executable>\n  </DTS:Executables>\n</DTS:Executable>"
    new_close = "    </DTS:Executable>\n      </DTS:Executables>\n    </DTS:Executable>\n  </DTS:Executables>\n</DTS:Executable>"
    assert text.count(old_close) == 1, "expected exactly one closing block to wrap"
    text = text.replace(old_close, new_close, 1)

    OUT.write_text(text)
    print(f"wrote {OUT} ({len(text.splitlines())} lines)")


if __name__ == "__main__":
    main()
