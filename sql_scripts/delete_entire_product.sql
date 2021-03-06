/* Delete an entire product from the database 
Note: Parent themes/subjects are not removed from gis.IndicatorTheme in case
they are still being used by other products */
DECLARE @pid INT;
SET @pid = 00000000; /* enter product id to delete */

delete from gis.DimensionValues where DimensionId in(
select DimensionId from gis.Dimensions where IndicatorThemeId=@pid)
delete from gis.Dimensions where IndicatorThemeId=@pid
delete from gis.IndicatorTheme where IndicatorThemeId=@pid

DELETE FROM gis.RelatedCharts WHERE RelatedChartId IN (
SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = @pid)
DELETE FROM gis.IndicatorMetaData WHERE IndicatorId IN (
SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = @pid)
DELETE FROM gis.IndicatorValues WHERE IndicatorValueId IN (
SELECT IndicatorValueId FROM gis.GeographyReferenceForIndicator WHERE IndicatorId IN 
               (SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = @pid)
) 
DELETE FROM gis.GeographyReferenceForIndicator WHERE IndicatorId in (
SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = @pid)
DELETE FROM gis.GeographicLevelForIndicator WHERE IndicatorId in  (
SELECT IndicatorId FROM gis.Indicator WHERE IndicatorThemeId = @pid)
DELETE FROM gis.Indicator WHERE IndicatorThemeId = @pid
