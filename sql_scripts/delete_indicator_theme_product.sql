/* Delete product from gis.IndicatorTheme, gis.Dimensions, gis.DimensionValues only.
This is useful when you want to rerun a product with the -i (insert) flag. Data in the
remaining tables will then be automatically overwritten by the append process.
Note: Parent themes/subjects are not removed from gis.IndicatorTheme in case
they are still being used by other products */
DECLARE @pid INT;
SET @pid = 00000000; /* enter product id to delete */

delete from gis.DimensionValues where DimensionId in(
select DimensionId from gis.Dimensions where IndicatorThemeId=@pid)
delete from gis.Dimensions where IndicatorThemeId=@pid
delete from gis.IndicatorTheme where IndicatorThemeId=@pid