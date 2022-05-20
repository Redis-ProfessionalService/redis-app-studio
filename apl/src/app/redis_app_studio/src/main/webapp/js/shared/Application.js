var AppContext = (function () {
    function AppContext(aGroupName, anAppName) {
        this.setAppName(anAppName);
        this.setGroupName(aGroupName);
        this.setAppType("CRUD+S");
        this.setDSStructure("Flat");
        this.setFetchPolicy("virtual");
        this.setCriteriaOffset(0);
        this.setCriteriaLimit(100);
        this.setGridHeightPercentage(80);
        this.setGridCSVHeader("title");
        this.setLoggingFlag(false);
        this.setFacetUIEnabled(false);
        this.setFacetValueCount(10);
        this.setRedisStorageType("Sorted Set of Hashes");
        this.rsFacetList = new Array();
        this.setHighlightFontColor("#FF00008F");
        this.setHighlightsAssigned(false);
        this.setAppViewRelDS("Undefined");
        this.setAppViewRelOutDS("Undefined");
        this.setGraphRelTypes(["Undefined"]);
        this.setGraphNodeLabels(["Undefined"]);
        this.setRedisInsightURL("http://localhost:8001/");
        this.setGraphVisualizationURL("http://localhost:8080/redis/isomorphic/visualize/show");
        this.objectMap = new Map();
    }
    AppContext.prototype.setGroupName = function (aGroupName) {
        this.groupName = aGroupName;
    };
    AppContext.prototype.getGroupName = function () {
        return this.groupName;
    };
    AppContext.prototype.setAppName = function (anAppName) {
        this.appName = anAppName;
    };
    AppContext.prototype.getAppName = function () {
        return this.appName;
    };
    AppContext.prototype.setAppType = function (anAppType) {
        this.appType = anAppType;
    };
    AppContext.prototype.getAppType = function () {
        return this.appType;
    };
    AppContext.prototype.setAppViewDS = function (aDSName) {
        this.dsAppViewDS = aDSName;
    };
    AppContext.prototype.getAppViewDS = function () {
        return this.dsAppViewDS;
    };
    AppContext.prototype.setAppViewTitle = function (aTitle) {
        this.dsAppViewTitle = aTitle;
    };
    AppContext.prototype.getAppViewTitle = function () {
        return this.dsAppViewTitle;
    };
    AppContext.prototype.setAppViewRelDS = function (aDSName) {
        this.dsAppViewRel = aDSName;
    };
    AppContext.prototype.getAppViewRelDS = function () {
        return this.dsAppViewRel;
    };
    AppContext.prototype.setAppViewRelOutDS = function (aDSName) {
        this.dsAppViewRelOut = aDSName;
    };
    AppContext.prototype.getAppViewRelOutDS = function () {
        return this.dsAppViewRelOut;
    };
    AppContext.prototype.isJsonEnabled = function () {
        return ((this.isStructureHierarchy()) && (this.dsAppViewRel === "Undefined") && (this.dsAppViewRelOut === "Undefined"));
    };
    AppContext.prototype.isGraphEnabled = function () {
        return ((this.isStructureHierarchy()) && (this.dsAppViewRel != "Undefined") && (this.dsAppViewRelOut != "Undefined"));
    };
    AppContext.prototype.isModelerEnabled = function () {
        return (this.dsAppViewTitle != "Application Launcher");
    };
    AppContext.prototype.setVersion = function (aVersion) {
        this.version = aVersion;
    };
    AppContext.prototype.getVersion = function () {
        return this.version;
    };
    AppContext.prototype.setPrefix = function (aPrefix) {
        this.prefix = aPrefix;
    };
    AppContext.prototype.getPrefix = function () {
        return this.prefix;
    };
    AppContext.prototype.setDSStructure = function (aStructure) {
        this.dsStructure = aStructure;
    };
    AppContext.prototype.getDSStructure = function () {
        return this.dsStructure;
    };
    AppContext.prototype.isStructureFlat = function () {
        return this.dsStructure == "Flat";
    };
    AppContext.prototype.isStructureHierarchy = function () {
        return this.dsStructure == "Hierarchy";
    };
    AppContext.prototype.setDSStorage = function (aStorage) {
        this.dsStorage = aStorage;
    };
    AppContext.prototype.getDSStorage = function () {
        return this.dsStorage;
    };
    AppContext.prototype.setCriteriaOffset = function (anOffset) {
        this.criteriaOffset = anOffset;
    };
    AppContext.prototype.getCriteriaOffset = function () {
        return this.criteriaOffset;
    };
    AppContext.prototype.setCriteriaLimit = function (aLimit) {
        this.criteriaLimit = aLimit;
    };
    AppContext.prototype.getCriteriaLimit = function () {
        return this.criteriaLimit;
    };
    AppContext.prototype.setGridHeightPercentage = function (aPercentage) {
        this.gridHeightPercentage = aPercentage;
    };
    AppContext.prototype.getGridHeightNumber = function () {
        return this.gridHeightPercentage;
    };
    AppContext.prototype.getGridHeightPercentage = function () {
        return this.gridHeightPercentage.toString() + "%";
    };
    AppContext.prototype.setGridCSVHeader = function (aHeader) {
        this.gridCSVHeader = aHeader;
    };
    AppContext.prototype.getGridCSVHeader = function () {
        return this.gridCSVHeader;
    };
    AppContext.prototype.setFetchPolicy = function (aFetchPolicy) {
        this.fetchPolicy = aFetchPolicy;
    };
    AppContext.prototype.getFetchPolicy = function () {
        return this.fetchPolicy;
    };
    AppContext.prototype.getAppPrefixDS = function (aDS) {
        return this.prefix + "-" + aDS;
    };
    AppContext.prototype.setRedisInsightURL = function (aURL) {
        this.redisInsightURL = aURL;
    };
    AppContext.prototype.getRedisInsightURL = function () {
        return this.redisInsightURL;
    };
    AppContext.prototype.setGraphVisualizationURL = function (aURL) {
        this.redisGraphURL = aURL;
    };
    AppContext.prototype.getGraphVisualizationURL = function () {
        return this.redisGraphURL;
    };
    AppContext.prototype.setAccountName = function (aAccountName) {
        this.accountName = aAccountName;
    };
    AppContext.prototype.getAccountName = function () {
        return this.accountName;
    };
    AppContext.prototype.setAccountPassword = function (aAccountPassword) {
        this.accountPassword = aAccountPassword;
    };
    AppContext.prototype.getAccountPassword = function () {
        return this.accountPassword;
    };
    AppContext.prototype.setRedisStorageType = function (aStorageType) {
        this.redisStorageType = aStorageType;
    };
    AppContext.prototype.getRedisStorageType = function () {
        return this.redisStorageType;
    };
    AppContext.prototype.setHighlightsAssigned = function (aFlag) {
        this.isHighlightsEnabled = aFlag;
    };
    AppContext.prototype.isHighlightsAssigned = function () {
        return this.isHighlightsEnabled;
    };
    AppContext.prototype.setHighlightFontColor = function (aColor) {
        this.highlightFontColor = aColor;
    };
    AppContext.prototype.getHighlightFontColor = function () {
        return this.highlightFontColor;
    };
    AppContext.prototype.setFacetUIEnabled = function (aFlag) {
        this.isFacetsEnabled = aFlag;
    };
    AppContext.prototype.isFacetUIEnabled = function () {
        return this.isFacetsEnabled;
    };
    AppContext.prototype.addFacetField = function (aFieldName) {
        this.rsFacetList.add(aFieldName);
    };
    AppContext.prototype.facetFieldExists = function (aFieldName) {
        for (var _i = 0, _a = this.rsFacetList; _i < _a.length; _i++) {
            var facetField = _a[_i];
            if (facetField === aFieldName)
                return true;
        }
        return false;
    };
    AppContext.prototype.removeFacetField = function (aFieldName) {
        this.rsFacetList.remove(aFieldName);
    };
    AppContext.prototype.getFacetFieldList = function () {
        return this.rsFacetList;
    };
    AppContext.prototype.clearFacetFieldList = function () {
        this.rsFacetList = new Array();
    };
    AppContext.prototype.setFacetValueCount = function (aValueCount) {
        this.facetValueCount = aValueCount;
    };
    AppContext.prototype.getFacetValueCount = function () {
        return this.facetValueCount;
    };
    AppContext.prototype.setGraphRelTypes = function (aRelTypes) {
        this.graphRelTypes = aRelTypes;
    };
    AppContext.prototype.getGraphRelTypes = function () {
        return this.graphRelTypes;
    };
    AppContext.prototype.setGraphNodeLabels = function (aLabelNames) {
        this.graphNodeLabels = aLabelNames;
    };
    AppContext.prototype.getGraphNodeLabels = function () {
        return this.graphNodeLabels;
    };
    AppContext.prototype.getContextValue = function () {
        return this.prefix + "|" + this.dsStructure + "|" + this.dsAppViewTitle;
    };
    AppContext.prototype.assignRecordContext = function (aRecord) {
        aRecord.ras_context = this.getContextValue();
    };
    AppContext.prototype.assignFormContext = function (aForm) {
        aForm.setValue("ras_context", this.getContextValue());
    };
    AppContext.prototype.simple13Rotation = function (aString) {
        var input = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
        var output = 'NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm';
        var index = function (x) { return input.indexOf(x); };
        var translate = function (x) { return index(x) > -1 ? output[index(x)] : x; };
        return aString.split('').map(translate).join('');
    };
    AppContext.prototype.setLoggingFlag = function (aIsLoggingEnabled) {
        this.isLoggingEnabled = aIsLoggingEnabled;
    };
    AppContext.prototype.getLoggingFlag = function () {
        return this.isLoggingEnabled;
    };
    AppContext.prototype.add = function (aName, anObject) {
        this.objectMap.set(aName, anObject);
    };
    AppContext.prototype.get = function (aName) {
        return this.objectMap.get(aName);
    };
    AppContext.prototype.getCookie = function (aName) {
        var docCookies = document.cookie.split(';');
        for (var i = 0; i < docCookies.length; i++) {
            var nameValuePair = docCookies[i].trim().split('=');
            if (nameValuePair[0] == aName)
                return nameValuePair[1];
        }
        return null;
    };
    return AppContext;
}());
var AppBuilder = (function () {
    function AppBuilder(aBaseName, anAppName) {
        this.appContext = new AppContext(aBaseName, anAppName);
    }
    AppBuilder.prototype.version = function (aVersion) {
        this.appContext.setVersion(aVersion);
        return this;
    };
    AppBuilder.prototype.prefix = function (aPrefix) {
        this.appContext.setPrefix(aPrefix);
        return this;
    };
    AppBuilder.prototype.appType = function (aType) {
        this.appContext.setAppType(aType);
        return this;
    };
    AppBuilder.prototype.dsStructure = function (aStructure) {
        this.appContext.setDSStructure(aStructure);
        return this;
    };
    AppBuilder.prototype.dsStorage = function (aStorage) {
        this.appContext.setDSStorage(aStorage);
        return this;
    };
    AppBuilder.prototype.pageSize = function (aPageSize) {
        this.appContext.setCriteriaLimit(aPageSize);
        return this;
    };
    AppBuilder.prototype.gridHeight = function (aHeight) {
        this.appContext.setGridHeightPercentage(aHeight);
        return this;
    };
    AppBuilder.prototype.accountName = function (aName) {
        this.appContext.setAccountName(aName);
        return this;
    };
    AppBuilder.prototype.accountPassword = function (aPassword) {
        this.appContext.setAccountPassword(aPassword);
        return this;
    };
    AppBuilder.prototype.dsAppViewName = function (aDSName) {
        this.appContext.setAppViewDS(aDSName);
        return this;
    };
    AppBuilder.prototype.dsAppViewTitle = function (aDataSourceTitle) {
        this.appContext.setAppViewTitle(aDataSourceTitle);
        return this;
    };
    AppBuilder.prototype.dsAppViewRel = function (aDSName) {
        this.appContext.setAppViewRelDS(aDSName);
        return this;
    };
    AppBuilder.prototype.dsAppViewRelOut = function (aDSName) {
        this.appContext.setAppViewRelOutDS(aDSName);
        return this;
    };
    AppBuilder.prototype.redisStorageType = function (aStorageType) {
        this.appContext.setRedisStorageType(aStorageType);
        return this;
    };
    AppBuilder.prototype.graphNodeLabels = function () {
        var anArgs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            anArgs[_i] = arguments[_i];
        }
        this.appContext.setGraphNodeLabels(anArgs);
        return this;
    };
    AppBuilder.prototype.graphRelTypes = function () {
        var anArgs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            anArgs[_i] = arguments[_i];
        }
        this.appContext.setGraphRelTypes(anArgs);
        return this;
    };
    AppBuilder.prototype.graphVisualizationURL = function (aURL) {
        this.appContext.setGraphVisualizationURL(aURL);
        return this;
    };
    AppBuilder.prototype.redisInsightURL = function (aURL) {
        this.appContext.setRedisInsightURL(aURL);
        return this;
    };
    AppBuilder.prototype.facetUIEnabled = function (aFacetsEnabled) {
        this.appContext.setFacetUIEnabled(aFacetsEnabled);
        return this;
    };
    AppBuilder.prototype.loggingEnabled = function (aLogEnabled) {
        this.appContext.setLoggingFlag(aLogEnabled);
        return this;
    };
    AppBuilder.prototype.build = function () {
        return this.appContext;
    };
    return AppBuilder;
}());
//# sourceMappingURL=Application.js.map