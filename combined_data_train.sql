.timer ON
CREATE TABLE IF NOT EXISTS SearchInfo_ AS SELECT
	a.SearchID,
	a.SearchDate, 
	a.IPID, a.UserID, 
	a.IsUserLoggedOn, 
	a.SearchQuery, 
	a.LocationID as SearchLocationID, 
	a.CategoryID as SearchCategoryID, 
	a.SearchParams, 
	b.Level as SearchLocationLevel, 
	b.RegionID as SearchRegionID, 
	b.CityID as SearchCityID, 
	c.Level as SearchCategoryLevel, 
	c.ParentCategoryID as SearchParentCategoryID, 
	c.SubcategoryID as SearchSubcategoryID 
FROM SearchInfo a 
LEFT OUTER JOIN Location b ON a.LocationID=b.LocationID
LEFT OUTER JOIN Category c ON a.CategoryID=c.CategoryID;

CREATE TABLE IF NOT EXISTS AdsInfo_ AS SELECT
	  a.AdID
	, a.LocationID as AdLocationID
	, a.CategoryID as AdCategoryID
	, a.Params
	, a.Price
	, a.Title
	, a.IsContext
	, b.Level as AdLocationLevel
	, b.RegionID as AdRegionID
	, b.CityID as AdCityID
	, c.Level as AdCategoryLevel
	, c.ParentCategoryID as AdParentCategoryID
	, c.SubcategoryID as AdSubcategoryID
FROM AdsInfo a 
LEFT OUTER JOIN Location b ON a.LocationID=b.LocationID
LEFT OUTER JOIN Category c ON a.CategoryID=c.CategoryID;

CREATE TABLE BigData5train AS SELECT
	  a.SearchID
	, a.AdID
	, a.Position
	, a.ObjectType
	, a.HistCTR
        , a.IsClick
	, b.SearchDate
	, b.IPID
	, b.UserID
	, b.IsUserLoggedOn
	, b.SearchLocationID
	, b.SearchCategoryID
	, b.SearchLocationLevel
	, b.SearchRegionID
	, b.SearchCityID
	, b.SearchCategoryLevel
	, b.SearchParentCategoryID
	, b.SearchSubcategoryID
	, c.AdLocationID
	, c.AdCategoryID
	, c.Price
	, c.AdLocationLevel
	, c.AdRegionID
	, c.AdCityID
	, c.AdCategoryLevel
	, c.AdParentCategoryID
	, c.AdSubcategoryID
	, d.UserAgentID 
	, d.UserAgentOSID
	, d.UserDeviceID 
	, d.UserAgentFamilyID
FROM trainSearchStream a 
LEFT OUTER JOIN SearchInfo_ b ON b.SearchID=a.SearchID
LEFT OUTER JOIN AdsInfo_ c ON c.AdID=a.AdID
LEFT OUTER JOIN UserInfo d ON d.UserID=b.UserID;

.timer off
.mode csv
.headers on
.output newdataset_train.csv
SELECT * from BigData5train;
.output stdout
