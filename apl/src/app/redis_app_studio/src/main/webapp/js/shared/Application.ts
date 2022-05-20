/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * The Application Context is responsible for capturing the configuration
 * state of any App Studio application.
 */
class AppContext
{
    protected appName: string;
    protected appType: string;                      // "CRUD+S", "Fraud Detection", "Ecommerce Search"
    protected groupName: string;
    protected version: string;
    protected prefix: string;
    protected dsStorage: string;                     // "Filesystem","RedisCore","RediSearch","RedisGraph","RedisJSON","RedisTimeSeries"
    protected dsStructure: string;                   // "Flat" or "Hierarchy"
    protected fetchPolicy: string;                   // "virtual" or "paging"
    protected accountName: string;
    protected dsAppViewDS: string;
    protected redisGraphURL: string;                 // Graph visual rendering service
    protected gridCSVHeader: string;                 // "title" or "field"
    protected dsAppViewRel: string;
    protected dsAppViewRelOut: string;
    protected dsAppViewTitle: string;
    protected criteriaLimit: number;
    protected criteriaOffset: number;
    protected redisInsightURL: string;               // RedisInsight service
    protected accountPassword: string;
    protected redisStorageType: string;              // specific to RedisCore applications
    protected facetValueCount: number;               // specific to RediSearch applications
    protected isFacetsEnabled: boolean;              // specific to RediSearch applications
    protected rsFacetList: Array<string>;            // specific to RediSearch applications
    protected graphRelTypes: string[];               // specific to RedisGraph applications
    protected graphNodeLabels: string[];             // specific to RedisGraph applications
    protected isLoggingEnabled: boolean;
    protected highlightFontColor: string;
    protected gridHeightPercentage: number;
    protected isHighlightsEnabled: boolean;
    protected objectMap: Map<string, any>;

    public constructor(aGroupName: string, anAppName: string)
    {
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
        this.rsFacetList = new Array<string>();
        this.setHighlightFontColor("#FF00008F");
        this.setHighlightsAssigned(false);
        this.setAppViewRelDS("Undefined");
        this.setAppViewRelOutDS("Undefined");
        this.setGraphRelTypes(["Undefined"]);
        this.setGraphNodeLabels(["Undefined"]);
        // Have the server-side app generation logic assign these in the HTML files they generate
        this.setRedisInsightURL("http://localhost:8001/");
        this.setGraphVisualizationURL("http://localhost:8080/redis/isomorphic/visualize/show");
        this.objectMap = new Map<string, any>();
    }

    public setGroupName(aGroupName: string)
    {
        this.groupName = aGroupName;
    }
    public getGroupName(): string
    {
        return this.groupName;
    }

    public setAppName(anAppName: string)
    {
        this.appName = anAppName;
    }
    public getAppName(): string
    {
        return this.appName;
    }

    public setAppType(anAppType: string)
    {
        this.appType = anAppType;
    }
    public getAppType(): string
    {
        return this.appType;
    }

    public setAppViewDS(aDSName: string)
    {
        this.dsAppViewDS = aDSName;
    }
    public getAppViewDS(): string
    {
        return this.dsAppViewDS;
    }

    public setAppViewTitle(aTitle: string)
    {
        this.dsAppViewTitle = aTitle;
    }
    public getAppViewTitle(): string
    {
        return this.dsAppViewTitle;
    }

    public setAppViewRelDS(aDSName: string)
    {
        this.dsAppViewRel = aDSName;
    }
    public getAppViewRelDS(): string
    {
        return this.dsAppViewRel;
    }

    public setAppViewRelOutDS(aDSName: string)
    {
        this.dsAppViewRelOut = aDSName;
    }
    public getAppViewRelOutDS(): string
    {
        return this.dsAppViewRelOut;
    }

    public isJsonEnabled(): boolean
    {
        return ((this.isStructureHierarchy()) && (this.dsAppViewRel === "Undefined") && (this.dsAppViewRelOut === "Undefined"))
    }
    public isGraphEnabled(): boolean
    {
        return ((this.isStructureHierarchy()) && (this.dsAppViewRel != "Undefined") && (this.dsAppViewRelOut != "Undefined"))
    }

    public isModelerEnabled(): boolean
    {
        return (this.dsAppViewTitle != "Application Launcher");
    }

    public setVersion(aVersion: string)
    {
        this.version = aVersion;
    }
    public getVersion(): string
    {
        return this.version;
    }

    public setPrefix(aPrefix: string)
    {
        this.prefix = aPrefix;
    }
    public getPrefix(): string
    {
        return this.prefix;
    }

    public setDSStructure(aStructure: string)
    {
        this.dsStructure = aStructure;
    }
    public getDSStructure(): string
    {
        return this.dsStructure;
    }
    public isStructureFlat(): boolean
    {
        return this.dsStructure == "Flat";
    }
    public isStructureHierarchy(): boolean
    {
        return this.dsStructure == "Hierarchy";
    }

    public setDSStorage(aStorage: string)
    {
        this.dsStorage = aStorage;
    }
    public getDSStorage(): string
    {
        return this.dsStorage;
    }

    public setCriteriaOffset(anOffset: number)
    {
        this.criteriaOffset = anOffset;
    }
    public getCriteriaOffset(): number
    {
        return this.criteriaOffset;
    }

    public setCriteriaLimit(aLimit: number)
    {
        this.criteriaLimit = aLimit;
    }
    public getCriteriaLimit(): number
    {
        return this.criteriaLimit;
    }

    public setGridHeightPercentage(aPercentage: number)
    {
        this.gridHeightPercentage = aPercentage;
    }
    public getGridHeightNumber(): number
    {
        return this.gridHeightPercentage;
    }
    public getGridHeightPercentage(): string
    {
        return this.gridHeightPercentage.toString() + "%";
    }

    public setGridCSVHeader(aHeader: string)
    {
        this.gridCSVHeader = aHeader;
    }
    public getGridCSVHeader(): string
    {
        return this.gridCSVHeader;
    }

    public setFetchPolicy(aFetchPolicy: string)
    {
        this.fetchPolicy = aFetchPolicy;
    }
    public getFetchPolicy(): string
    {
        return this.fetchPolicy;
    }

    public getAppPrefixDS(aDS: string): string
    {
        return this.prefix + "-" + aDS;
    }

    public setRedisInsightURL(aURL: string)
    {
        this.redisInsightURL = aURL;
    }
    public getRedisInsightURL(): string
    {
        return this.redisInsightURL;
    }

    public setGraphVisualizationURL(aURL: string)
    {
        this.redisGraphURL = aURL;
    }
    public getGraphVisualizationURL(): string
    {
        return this.redisGraphURL;
    }

    public setAccountName(aAccountName: string)
    {
        this.accountName = aAccountName;
    }
    public getAccountName(): string
    {
        return this.accountName;
    }

    public setAccountPassword(aAccountPassword: string)
    {
        this.accountPassword = aAccountPassword;
    }
    public getAccountPassword(): string
    {
        return this.accountPassword;
    }

    public setRedisStorageType(aStorageType: string)
    {
        this.redisStorageType = aStorageType;
    }
    public getRedisStorageType(): string
    {
        return this.redisStorageType;
    }

    public setHighlightsAssigned(aFlag: boolean)
    {
        this.isHighlightsEnabled = aFlag;
    }
    public isHighlightsAssigned(): boolean
    {
        return this.isHighlightsEnabled;
    }

    public setHighlightFontColor(aColor: string)
    {
        this.highlightFontColor = aColor;
    }
    public getHighlightFontColor(): string
    {
        return this.highlightFontColor;
    }

    public setFacetUIEnabled(aFlag: boolean)
    {
        this.isFacetsEnabled = aFlag;
    }
    public isFacetUIEnabled(): boolean
    {
        return this.isFacetsEnabled;
    }

    public addFacetField(aFieldName: string): void
    {
        this.rsFacetList.add(aFieldName);
    }
    public facetFieldExists(aFieldName: string): boolean
    {
        for (let facetField of this.rsFacetList)
        {
            if (facetField === aFieldName)
                return true;
        }
        return false;
    }
    public removeFacetField(aFieldName: string): void
    {
        this.rsFacetList.remove(aFieldName);
    }
    public getFacetFieldList(): Array<string>
    {
        return this.rsFacetList;
    }
    public clearFacetFieldList(): void
    {
        this.rsFacetList = new Array<string>();
    }

    public setFacetValueCount(aValueCount: number): void
    {
        this.facetValueCount = aValueCount;
    }
    public getFacetValueCount(): number
    {
        return this.facetValueCount;
    }

    public setGraphRelTypes(aRelTypes: string[]): void
    {
        this.graphRelTypes = aRelTypes;
    }
    public getGraphRelTypes() :string[]
    {
        return this.graphRelTypes;
    }

    public setGraphNodeLabels(aLabelNames: string[]): void
    {
        this.graphNodeLabels = aLabelNames;
    }
    public getGraphNodeLabels(): string[]
    {
        return this.graphNodeLabels;
    }

    private getContextValue(): string
    {
        return this.prefix + "|" + this.dsStructure + "|" + this.dsAppViewTitle;
    }
    public assignRecordContext(aRecord: isc.ListGridRecord): void
    {
        // @ts-ignore
        aRecord.ras_context = this.getContextValue();
    }
    public assignFormContext(aForm: isc.DynamicForm): void
    {
        aForm.setValue("ras_context", this.getContextValue());
    }

    private simple13Rotation(aString: string): string
    {
        let input     = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
        let output    = 'NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm';
        let index     = (x: string) => input.indexOf(x);
        let translate = (x: string) => index(x) > -1 ? output[index(x)] : x;
        return aString.split('').map(translate).join('');
    }

    public setLoggingFlag(aIsLoggingEnabled: boolean)
    {
        this.isLoggingEnabled = aIsLoggingEnabled;
    }
    public getLoggingFlag(): boolean
    {
        return this.isLoggingEnabled;
    }

    public add(aName: string, anObject: any)
    {
        this.objectMap.set(aName, anObject);
    }
    public get(aName: string) : any
    {
        return this.objectMap.get(aName);
    }

    public getCookie(aName: string) : string
    {
        let docCookies = document.cookie.split(';');
        for (let i = 0; i < docCookies.length; i++)
        {
            let nameValuePair = docCookies[i].trim().split('=');
            if (nameValuePair[0] == aName)
                return nameValuePair[1];
        }
        return null;
    }
}

// Convenience builder class for the application context
class AppBuilder
{
    private appContext: AppContext;

    public constructor(aBaseName: string, anAppName: string)
    {
        this.appContext = new AppContext(aBaseName, anAppName);
    }

    public version(aVersion: string): AppBuilder
    {
        this.appContext.setVersion(aVersion);
        return this;
    }
    public prefix(aPrefix: string): AppBuilder
    {
        this.appContext.setPrefix(aPrefix);
        return this;
    }
    public appType(aType: string): AppBuilder
    {
        this.appContext.setAppType(aType);
        return this;
    }
    public dsStructure(aStructure: string): AppBuilder
    {
        this.appContext.setDSStructure(aStructure);
        return this;
    }
    public dsStorage(aStorage: string): AppBuilder
    {
        this.appContext.setDSStorage(aStorage);
        return this;
    }
    public pageSize(aPageSize: number): AppBuilder
    {
        this.appContext.setCriteriaLimit(aPageSize);
        return this;
    }
    public gridHeight(aHeight: number): AppBuilder
    {
        this.appContext.setGridHeightPercentage(aHeight);
        return this;
    }
    public accountName(aName: string): AppBuilder
    {
        this.appContext.setAccountName(aName);
        return this;
    }
    public accountPassword(aPassword: string): AppBuilder
    {
        this.appContext.setAccountPassword(aPassword);
        return this;
    }
    public dsAppViewName(aDSName: string): AppBuilder
    {
        this.appContext.setAppViewDS(aDSName);
        return this;
    }
    public dsAppViewTitle(aDataSourceTitle: string): AppBuilder
    {
        this.appContext.setAppViewTitle(aDataSourceTitle);
        return this;
    }
    public dsAppViewRel(aDSName: string): AppBuilder
    {
        this.appContext.setAppViewRelDS(aDSName);
        return this;
    }
    public dsAppViewRelOut(aDSName: string): AppBuilder
    {
        this.appContext.setAppViewRelOutDS(aDSName);
        return this;
    }
    public redisStorageType(aStorageType: string): AppBuilder
    {
        this.appContext.setRedisStorageType(aStorageType);
        return this;
    }
    public graphNodeLabels(...anArgs: string[]): AppBuilder
    {
        this.appContext.setGraphNodeLabels(anArgs);
        return this;
    }
    public graphRelTypes(...anArgs: string[]): AppBuilder
    {
        this.appContext.setGraphRelTypes(anArgs);
        return this;
    }
    public graphVisualizationURL(aURL: string): AppBuilder
    {
        this.appContext.setGraphVisualizationURL(aURL);
        return this;
    }
    public redisInsightURL(aURL: string): AppBuilder
    {
        this.appContext.setRedisInsightURL(aURL);
        return this;
    }
    public facetUIEnabled(aFacetsEnabled: boolean): AppBuilder
    {
        this.appContext.setFacetUIEnabled(aFacetsEnabled);
        return this;
    }
    public loggingEnabled(aLogEnabled: boolean): AppBuilder
    {
        this.appContext.setLoggingFlag(aLogEnabled);
        return this;
    }

    public build(): AppContext
    {
        return this.appContext;
    }
}