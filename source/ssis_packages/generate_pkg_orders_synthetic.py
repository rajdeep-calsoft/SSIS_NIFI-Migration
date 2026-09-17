#!/usr/bin/env python3
"""Generates pkg_orders_synthetic.dtsx -- a real, converter-targeted SSIS
package, built entirely from scratch (no dependency on any other .dtsx file
-- unlike generate_pkg_full_coverage_synthetic.py, which reads
pkg_orders_etl.dtsx as its base and breaks if that file is ever removed).

Business shape: ingest one landing file of order lines, validate against the
real reference tables, load clean lines into order_items and a rolled-up
header into orders, quarantine everything else with a reason. Exactly the 4
reject reasons destination/spec/fixtures/make_fixture.py's
tier3-bulk-generated fixture exercises (UNKNOWN_SKU, UNKNOWN_CUSTOMER,
RANGE_VIOLATION, BAD_CURRENCY), so this package can be verified against that
fixture with no new fixture-building work.

Every property/expression string below is copied verbatim from
pkg_full_coverage_synthetic.dtsx's own matching component -- that file
converts at 100% and is deployed live, so these are proven-correct shapes,
not new guesses.

.dtsx files are never hand-edited (CLAUDE.md): edit this script and re-run
it, the same relationship every other generate_*.py has to its .dtsx.
"""
from __future__ import annotations

import pathlib

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "pkg_orders_synthetic.dtsx"

DF = r"Package\Batch Loop\Ingest Orders"
SRC_OUT = f"{DF}\\Extract Order Lines.Outputs[Flat File Source Output]"


def source_col(name: str, dtype: str) -> str:
    return (f'                    <outputColumn refId="{SRC_OUT}.Columns[{name}]" name="{name}" '
            f'dataType="{dtype}" lineageId="{SRC_OUT}.Columns[{name}]" />\n')


def lookup_component(name: str, sql_table: str, key_col: str, cache_type: str,
                      match_cols: list[tuple[str, str]]) -> str:
    """A Microsoft.Lookup component, joining on key_col against sql_table."""
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
                <connection refId="{DF}\\{name}.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\{name}.Inputs[Lookup Input]" name="Lookup Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[Lookup Input].Columns[{key_col}]" cachedName="{key_col}" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[{key_col}]">
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


def tag_component(name: str, reason: str) -> str:
    """A Microsoft.DerivedColumn that stamps a literal `reason` string."""
    return f'''            <component refId="{DF}\\{name}" componentClassID="Microsoft.DerivedColumn" name="{name}" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\{name}.Inputs[Derived Column Input]" name="Derived Column Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[Derived Column Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[order_id]" />
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
    """A Microsoft.OLEDBDestination into quarantine_records, fed by order_id
    (from the source) and reason (from the matching Tag component)."""
    return f'''            <component refId="{DF}\\{name}" componentClassID="Microsoft.OLEDBDestination" name="{name}" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[quarantine_records]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\{name}.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\{name}.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\{name}.Inputs[OLE DB Destination Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[order_id]" />
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
            ("order_id", "wstr"), ("line_no", "i4"), ("customer_id", "wstr"),
            ("order_ts", "i8"), ("status", "wstr"), ("channel", "wstr"),
            ("currency", "wstr"), ("sku", "wstr"), ("qty", "i4"),
            ("unit_price", "r8"), ("line_total", "r8"),
        ]
    )

    extract = f'''            <component refId="{DF}\\Extract Order Lines" componentClassID="Microsoft.FlatFileSource" name="Extract Order Lines" usesDispositions="true" version="1">
              <properties />
              <connections>
                <connection refId="{DF}\\Extract Order Lines.Connections[FlatFileConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Landing]" name="FlatFileConnection" />
              </connections>
              <outputs>
                <output refId="{SRC_OUT}" name="Flat File Source Output">
                  <outputColumns>
{source_cols}                  </outputColumns>
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\Extract Order Lines.Outputs[Flat File Source Error Output]" name="Flat File Source Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    lookup_product = lookup_component(
        "Lookup Product", "products where unit_price > 0", "sku", "1",
        [("category", "wstr")],
    )
    lookup_customer = lookup_component(
        "Lookup Customer", "customers", "customer_id", "0",
        [("country", "wstr"), ("segment", "wstr")],
    )

    business_rules = f'''            <component refId="{DF}\\Business Rules" componentClassID="Microsoft.ConditionalSplit" name="Business Rules" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\Business Rules.Inputs[Conditional Split Input]" name="Conditional Split Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Business Rules.Inputs[Conditional Split Input].Columns[qty]" cachedName="qty" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[qty]" />
                    <inputColumn refId="{DF}\\Business Rules.Inputs[Conditional Split Input].Columns[unit_price]" cachedName="unit_price" cachedDataType="r8" lineageId="{SRC_OUT}.Columns[unit_price]" />
                    <inputColumn refId="{DF}\\Business Rules.Inputs[Conditional Split Input].Columns[currency]" cachedName="currency" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[currency]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Business Rules.Outputs[range_violation]" name="range_violation">
                  <properties>
                    <property name="FriendlyExpression" dataType="System.String">!(qty &gt;= 1 &amp;&amp; qty &lt;= 999 &amp;&amp; unit_price &gt;= 0.01 &amp;&amp; unit_price &lt;= 9999.99)</property>
                    <property name="Order" dataType="System.String">0</property>
                  </properties>
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\Business Rules.Outputs[bad_currency]" name="bad_currency">
                  <properties>
                    <property name="FriendlyExpression" dataType="System.String">(qty &gt;= 1 &amp;&amp; qty &lt;= 999 &amp;&amp; unit_price &gt;= 0.01 &amp;&amp; unit_price &lt;= 9999.99) &amp;&amp; !(UPPER(currency) == "INR" || UPPER(currency) == "USD" || UPPER(currency) == "EUR" || UPPER(currency) == "GBP")</property>
                    <property name="Order" dataType="System.String">1</property>
                  </properties>
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
                <output refId="{DF}\\Business Rules.Outputs[clean]" name="clean">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    aggregate = f'''            <component refId="{DF}\\Aggregate Order Rollup" componentClassID="Microsoft.Aggregate" name="Aggregate Order Rollup" usesDispositions="true" version="1">
              <properties />
              <inputs>
                <input refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input]" name="Aggregate Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[order_id]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[customer_id]" cachedName="customer_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[customer_id]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[order_ts]" cachedName="order_ts" cachedDataType="i8" lineageId="{SRC_OUT}.Columns[order_ts]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[status]" cachedName="status" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[status]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[channel]" cachedName="channel" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[channel]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[currency]" cachedName="currency" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[currency]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[qty]" cachedName="qty" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[qty]" />
                    <inputColumn refId="{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input].Columns[line_total]" cachedName="line_total" cachedDataType="r8" lineageId="{SRC_OUT}.Columns[line_total]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1]" name="Aggregate Output 1">
                  <outputColumns>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]" name="order_id" dataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">GroupBy</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]" name="customer_id" dataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">customer_id</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]" name="order_ts" dataType="i8" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">order_ts</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]" name="status" dataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">status</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]" name="channel" dataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">channel</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]" name="currency" dataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Max</property>
                        <property name="SourceColumn" dataType="System.String">currency</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]" name="order_total" dataType="r8" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]">
                      <properties>
                        <property name="AggregationType" dataType="System.String">Sum</property>
                        <property name="SourceColumn" dataType="System.String">line_total</property>
                      </properties>
                    </outputColumn>
                    <outputColumn refId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]" name="item_count" dataType="i4" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]">
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
'''

    load_orders = f'''            <component refId="{DF}\\Load Orders Rollup" componentClassID="Microsoft.OLEDBDestination" name="Load Orders Rollup" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[orders]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\Load Orders Rollup.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_id]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[customer_id]" cachedName="customer_id" cachedDataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[customer_id]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_ts]" cachedName="order_ts" cachedDataType="i8" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_ts]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[status]" cachedName="status" cachedDataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[status]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[channel]" cachedName="channel" cachedDataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[channel]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[currency]" cachedName="currency" cachedDataType="wstr" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[currency]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[order_total]" cachedName="order_total" cachedDataType="r8" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[order_total]" />
                    <inputColumn refId="{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input].Columns[item_count]" cachedName="item_count" cachedDataType="i4" lineageId="{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1].Columns[item_count]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Load Orders Rollup.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    load_items = f'''            <component refId="{DF}\\Load Order Items" componentClassID="Microsoft.OLEDBDestination" name="Load Order Items" usesDispositions="true" version="1">
              <properties>
                <property name="OpenRowset" dataType="System.String">[public].[order_items]</property>
                <property name="AccessMode" dataType="System.String">3</property>
                <property name="FastLoadOptions" dataType="System.String">TABLOCK,CHECK_CONSTRAINTS</property>
              </properties>
              <connections>
                <connection refId="{DF}\\Load Order Items.Connections[OleDbConnection]" connectionManagerRefId="Package.ConnectionManagers[Orders Warehouse]" name="OleDbConnection" />
              </connections>
              <inputs>
                <input refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input]" name="OLE DB Destination Input">
                  <inputColumns>
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[order_id]" cachedName="order_id" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[order_id]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[line_no]" cachedName="line_no" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[line_no]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[sku]" cachedName="sku" cachedDataType="wstr" lineageId="{SRC_OUT}.Columns[sku]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[qty]" cachedName="qty" cachedDataType="i4" lineageId="{SRC_OUT}.Columns[qty]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[unit_price]" cachedName="unit_price" cachedDataType="r8" lineageId="{SRC_OUT}.Columns[unit_price]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[line_total]" cachedName="line_total" cachedDataType="r8" lineageId="{SRC_OUT}.Columns[line_total]" />
                    <inputColumn refId="{DF}\\Load Order Items.Inputs[OLE DB Destination Input].Columns[category]" cachedName="category" cachedDataType="wstr" lineageId="{DF}\\Lookup Product.Outputs[Lookup Match Output].Columns[category]" />
                  </inputColumns>
                  <externalMetadataColumns />
                </input>
              </inputs>
              <outputs>
                <output refId="{DF}\\Load Order Items.Outputs[OLE DB Destination Error Output]" name="OLE DB Destination Error Output" isErrorOut="true">
                  <outputColumns />
                  <externalMetadataColumns />
                </output>
              </outputs>
            </component>
'''

    components = (
        extract + lookup_product +
        tag_component("Tag Unknown SKU", "UNKNOWN_SKU") +
        reject_component("Reject Unknown SKU", f"{DF}\\Tag Unknown SKU.Outputs[Derived Column Output].Columns[reason]") +
        lookup_customer +
        tag_component("Tag Unknown Customer", "UNKNOWN_CUSTOMER") +
        reject_component("Reject Unknown Customer", f"{DF}\\Tag Unknown Customer.Outputs[Derived Column Output].Columns[reason]") +
        business_rules +
        tag_component("Tag Range Violation", "RANGE_VIOLATION") +
        reject_component("Reject Range Violation", f"{DF}\\Tag Range Violation.Outputs[Derived Column Output].Columns[reason]") +
        tag_component("Tag Bad Currency", "BAD_CURRENCY") +
        reject_component("Reject Bad Currency", f"{DF}\\Tag Bad Currency.Outputs[Derived Column Output].Columns[reason]") +
        aggregate + load_orders + load_items
    )

    def path(name: str, start: str, end: str) -> str:
        return (f'            <path refId="{DF}.Paths[{name}]" name="{name}" '
                f'startId="{start}" endId="{end}" />\n')

    paths = (
        path("Flat File Source Output -&gt; Lookup Product",
             f"{DF}\\Extract Order Lines.Outputs[Flat File Source Output]",
             f"{DF}\\Lookup Product.Inputs[Lookup Input]") +
        path("Lookup No Match Output -&gt; Tag Unknown SKU",
             f"{DF}\\Lookup Product.Outputs[Lookup No Match Output]",
             f"{DF}\\Tag Unknown SKU.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Unknown SKU",
             f"{DF}\\Tag Unknown SKU.Outputs[Derived Column Output]",
             f"{DF}\\Reject Unknown SKU.Inputs[OLE DB Destination Input]") +
        path("Lookup Match Output -&gt; Lookup Customer",
             f"{DF}\\Lookup Product.Outputs[Lookup Match Output]",
             f"{DF}\\Lookup Customer.Inputs[Lookup Input]") +
        path("Lookup No Match Output -&gt; Tag Unknown Customer",
             f"{DF}\\Lookup Customer.Outputs[Lookup No Match Output]",
             f"{DF}\\Tag Unknown Customer.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Unknown Customer",
             f"{DF}\\Tag Unknown Customer.Outputs[Derived Column Output]",
             f"{DF}\\Reject Unknown Customer.Inputs[OLE DB Destination Input]") +
        path("Lookup Match Output -&gt; Business Rules",
             f"{DF}\\Lookup Customer.Outputs[Lookup Match Output]",
             f"{DF}\\Business Rules.Inputs[Conditional Split Input]") +
        path("range_violation -&gt; Tag Range Violation",
             f"{DF}\\Business Rules.Outputs[range_violation]",
             f"{DF}\\Tag Range Violation.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Range Violation",
             f"{DF}\\Tag Range Violation.Outputs[Derived Column Output]",
             f"{DF}\\Reject Range Violation.Inputs[OLE DB Destination Input]") +
        path("bad_currency -&gt; Tag Bad Currency",
             f"{DF}\\Business Rules.Outputs[bad_currency]",
             f"{DF}\\Tag Bad Currency.Inputs[Derived Column Input]") +
        path("Derived Column Output -&gt; Reject Bad Currency",
             f"{DF}\\Tag Bad Currency.Outputs[Derived Column Output]",
             f"{DF}\\Reject Bad Currency.Inputs[OLE DB Destination Input]") +
        path("clean -&gt; Load Order Items",
             f"{DF}\\Business Rules.Outputs[clean]",
             f"{DF}\\Load Order Items.Inputs[OLE DB Destination Input]") +
        path("clean -&gt; Aggregate Order Rollup",
             f"{DF}\\Business Rules.Outputs[clean]",
             f"{DF}\\Aggregate Order Rollup.Inputs[Aggregate Input]") +
        path("Aggregate Output 1 -&gt; Load Orders Rollup",
             f"{DF}\\Aggregate Order Rollup.Outputs[Aggregate Output 1]",
             f"{DF}\\Load Orders Rollup.Inputs[OLE DB Destination Input]")
    )

    text = f'''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts" DTS:refId="Package" DTS:CreationName="Microsoft.Package" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000001}}" DTS:ExecutableType="Microsoft.Package" DTS:LocaleID="1033" DTS:ObjectName="Orders Synthetic" DTS:PackageType="5" DTS:VersionBuild="1" DTS:VersionGUID="{{C7DE0001-0000-0000-0000-000000000002}}">
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[Orders Landing]" DTS:CreationName="FLATFILE" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000010}}" DTS:ObjectName="Orders Landing">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="/opt/nifi/data/landing/*.ndjson" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[Orders Warehouse]" DTS:CreationName="OLEDB" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000011}}" DTS:ObjectName="Orders Warehouse">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="Data Source=localhost;Initial Catalog=OrdersDW;Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  <DTS:Variables />
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\Batch Loop" DTS:CreationName="STOCK:FOREACHLOOP" DTS:Description="Loop for each landing file" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000030}}" DTS:ExecutableType="STOCK:FOREACHLOOP" DTS:LocaleID="1033" DTS:ObjectName="Batch Loop">
      <DTS:ForEachEnumerator DTS:CreationName="Microsoft.ForEachFileEnumerator" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000031}}" DTS:ObjectName="Foreach File Enumerator">
        <DTS:ObjectData>
          <ForEachFileEnumeratorProperties>
            <FEFEProperty Folder="/opt/nifi/data/landing" />
            <FEFEProperty FileSpec="*.ndjson" />
          </ForEachFileEnumeratorProperties>
        </DTS:ObjectData>
      </DTS:ForEachEnumerator>
      <DTS:Variables />
      <DTS:Executables>
    <DTS:Executable DTS:refId="{DF}" DTS:CreationName="Microsoft.Pipeline" DTS:Description="Data Flow Task" DTS:DTSID="{{C7DE0001-0000-0000-0000-000000000020}}" DTS:ExecutableType="Microsoft.Pipeline" DTS:LocaleID="1033" DTS:ObjectName="Ingest Orders">
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
