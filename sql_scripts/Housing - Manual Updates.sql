/* CONFIRM BEFORE USING ON REAL DATA */

SELECT TOP (1000) * FROM gis.IndicatorTheme;

/* Clean Subject Codes loaded by default */
DELETE FROM gis.IndicatorTheme WHERE ParentThemeId = 46;

/* Replace by client specified subject codes */
INSERT INTO gis.IndicatorTheme VALUES (4609, 'Property characteristics', 'Caractéristiques de la propriété', NULL, 'Property characteristics', 'Caractéristiques de la propriété', 46, 'C');
INSERT INTO gis.IndicatorTheme VALUES (4610, 'Ownership characteristics', 'Caractéristiques du propriétaire', NULL, 'Ownership characteristics', 'Caractéristiques du propriétaire', 46, 'C');

/* ASsign product ids to client specified subject codes */
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100018;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100019;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100027;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100028;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100029;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100030;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100038;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100039;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100040;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100041;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100047;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100048;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100049;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100051;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4610 WHERE IndicatorThemeId = 46100052;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100053;
UPDATE gis.IndicatorTheme SET ParentThemeId = 4609 WHERE IndicatorThemeId = 46100054;

/* Add product id in square brackets in front of product id */
UPDATE gis.IndicatorTheme SET IndicatorTheme_EN = CONCAT('[', IndicatorThemeId, '] ', IndicatorTheme_EN) WHERE ParentThemeId IN (4609, 4610);
UPDATE gis.IndicatorTheme SET IndicatorTheme_FR = CONCAT('[', IndicatorThemeId, '] ', IndicatorTheme_FR) WHERE ParentThemeId IN (4609, 4610);