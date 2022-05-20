var RediSearchJson = (function () {
    function RediSearchJson(anAppContext) {
        this.appContext = anAppContext;
    }
    RediSearchJson.prototype.createHTMLHeader = function () {
        var windowAppContext = window._appContext_;
        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis App Studio\" class=\"ahImage\" src=\"images/redis-app-studio.svg\" height=\"99\" width=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + windowAppContext.getAppName() + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    };
    RediSearchJson.prototype.createHeaderSection = function () {
        var htmlString = this.createHTMLHeader();
        var headerHTMLFlow = isc.HTMLFlow.create({ ID: "headerHTMLFlow", width: "100%", height: "5%", autoDraw: false, contents: htmlString });
        return headerHTMLFlow;
    };
    RediSearchJson.prototype.defaultCriteria = function (aDSStructure, aDSTitle, aFetchLimit) {
        var windowAppContext = window._appContext_;
        var fetchLimit;
        if (aFetchLimit)
            fetchLimit = aFetchLimit;
        else
            fetchLimit = 100;
        var simpleCriteria = {
            _dsTitle: aDSTitle,
            _dsStructure: aDSStructure,
            _appPrefix: windowAppContext.getPrefix(),
            _fetchPolicy: windowAppContext.getFetchPolicy(),
            _offset: windowAppContext.getCriteriaOffset(),
            _limit: fetchLimit
        };
        return simpleCriteria;
    };
    RediSearchJson.prototype.executeAppViewGridExport = function (anAction, aFormat, aFetchLimit) {
        var windowAppContext = window._appContext_;
        var windowAppViewGrid = window.appViewGrid;
        var appViewGrid;
        appViewGrid = windowAppViewGrid;
        var windowSearchForm = window.searchForm;
        var searchForm;
        searchForm = windowSearchForm;
        var windowSearchFilter = window.searchFilter;
        var searchFilter;
        searchFilter = windowSearchFilter;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_action", anAction);
        filterMap.set("_format", aFormat);
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", aFetchLimit);
        if (windowAppContext.isFacetUIEnabled()) {
            var facetNameValues_1 = String();
            var facetList = windowAppContext.getFacetFieldList();
            if (facetList.length > 0) {
                facetList.forEach(function (value) {
                    facetNameValues_1 += value + "|";
                });
                filterMap.set("_facetNameValues", facetNameValues_1);
            }
        }
        var searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        var acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        var acJSON = isc.JSON.encode(acFlattened);
        if (acJSON.length > 89)
            filterMap.set("_advancedCriteria", acJSON);
        var simpleCriteria = {};
        filterMap.forEach(function (value, key) {
            simpleCriteria[key] = value;
        });
        var dsRequest = {
            ID: "dsRequest",
            operationId: "exportData",
            downloadResult: true,
            downloadToNewWindow: false
        };
        appViewGrid.fetchData(simpleCriteria, null, dsRequest);
        setTimeout(function () { appViewGrid.filterData(simpleCriteria); }, 2000);
    };
    RediSearchJson.prototype.resetFacetList = function () {
        var windowAppContext = window._appContext_;
        var windowAppFacetGrid = window.appFacetGrid;
        var appFacetGrid;
        appFacetGrid = windowAppFacetGrid;
        var facetArray = [{ id: "1", parent_id: "0", facet_name: "Facet List" }];
        var facetDataTree = isc.Tree.create({ modelType: "parent", nameProperty: "facet_name", idField: "id", parentIdField: "parent_id", data: facetArray });
        appFacetGrid.invalidateCache();
        appFacetGrid.setData(facetDataTree);
        windowAppContext.clearFacetFieldList();
    };
    RediSearchJson.prototype.adjustFacetName = function (aRecord) {
        var windowAppContext = window._appContext_;
        var facetCountName = String(aRecord.facet_name);
        var offset = facetCountName.lastIndexOf(" (");
        if (offset > 0) {
            var facetFieldName = aRecord.field_name;
            var facetFieldValue = facetCountName.substring(0, offset);
            var facetFieldNameValue = facetFieldName + ":" + facetFieldValue;
            if (windowAppContext.facetFieldExists(facetFieldNameValue))
                return facetFieldValue;
        }
        return facetCountName;
    };
    RediSearchJson.prototype.assignFacetsCallback = function () {
        var windowFacetGrid = window.facetGrid;
        var facetGrid;
        facetGrid = windowFacetGrid;
        var windowAppFacetGrid = window.appFacetGrid;
        var appFacetGrid;
        appFacetGrid = windowAppFacetGrid;
        var facetArray = [];
        var rowCount = facetGrid.getTotalRows();
        if (rowCount > 0) {
            var _loop_1 = function (row) {
                var facetFieldMap = new Map();
                var lgRecord = facetGrid.getRecord(row);
                facetFieldMap.set("id", lgRecord.id);
                facetFieldMap.set("parent_id", lgRecord.parent_id);
                facetFieldMap.set("facet_name", RediSearchJson.prototype.adjustFacetName(lgRecord));
                var facetObject = {};
                facetFieldMap.forEach(function (value, key) {
                    facetObject[key] = value;
                });
                facetArray.add(facetObject);
            };
            for (var row = 0; row < rowCount; row++) {
                _loop_1(row);
            }
        }
        else
            facetArray = [{ id: "1", parent_id: "0", facet_name: "Schema Facets Undefined" }];
        var facetDataTree = isc.Tree.create({
            modelType: "parent", nameProperty: "facet_name",
            idField: "id", parentIdField: "parent_id",
            data: facetArray
        });
        appFacetGrid.invalidateCache();
        appFacetGrid.setData(facetDataTree);
        appFacetGrid.getData().openAll();
    };
    RediSearchJson.prototype.fetchFacetsCallback = function () {
        var minAdvancedCriteriaLength = 89;
        var windowAppContext = window._appContext_;
        if (windowAppContext.isFacetUIEnabled()) {
            var filterMap = new Map();
            filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
            filterMap.set("_dsStructure", windowAppContext.getDSStructure());
            filterMap.set("_appPrefix", windowAppContext.getPrefix());
            filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
            filterMap.set("_offset", windowAppContext.getCriteriaOffset());
            filterMap.set("_limit", windowAppContext.getCriteriaLimit());
            filterMap.set("_facetValueCount", windowAppContext.getFacetValueCount());
            var facetNameValues_2 = String();
            var facetList = windowAppContext.getFacetFieldList();
            if (facetList.length > 0) {
                facetList.forEach(function (value) {
                    facetNameValues_2 += value + "|";
                });
                filterMap.set("_facetNameValues", facetNameValues_2);
            }
            var windowSearchForm = window.searchForm;
            var searchForm = void 0;
            searchForm = windowSearchForm;
            var windowSearchFilter = window.searchFilter;
            var searchFilter = void 0;
            searchFilter = windowSearchFilter;
            var searchTerm = searchForm.getValue("search_terms");
            filterMap.set("_search", searchTerm);
            var acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
            var acJSON = isc.JSON.encode(acFlattened);
            if (acJSON.length > minAdvancedCriteriaLength)
                filterMap.set("_advancedCriteria", acJSON);
            var simpleCriteria_1 = {};
            filterMap.forEach(function (value, key) {
                simpleCriteria_1[key] = value;
            });
            var windowAppViewGrid = window.facetGrid;
            var facetGrid = void 0;
            facetGrid = windowAppViewGrid;
            facetGrid.invalidateCache();
            facetGrid.filterData(simpleCriteria_1, function (aDSResponse, aData, aDSRequest) {
                RediSearchJson.prototype.assignFacetsCallback();
            });
        }
    };
    RediSearchJson.prototype.executeAppViewGridSearch = function () {
        var minAdvancedCriteriaLength = 89;
        var windowAppContext = window._appContext_;
        var windowHighlightSearch = window.tsHighlightSearch;
        var tsHighlightSearch = isc.ToolStripButton;
        tsHighlightSearch = windowHighlightSearch;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        if (windowAppContext.isFacetUIEnabled()) {
            var facetNameValues_3 = String();
            var facetList = windowAppContext.getFacetFieldList();
            if (facetList.length > 0) {
                facetList.forEach(function (value) {
                    facetNameValues_3 += value + "|";
                });
                filterMap.set("_facetNameValues", facetNameValues_3);
            }
        }
        if (tsHighlightSearch.isSelected())
            filterMap.set("_highlight", "true");
        else
            filterMap.set("_highlight", "false");
        var windowSearchForm = window.searchForm;
        var searchForm;
        searchForm = windowSearchForm;
        var windowSearchFilter = window.searchFilter;
        var searchFilter;
        searchFilter = windowSearchFilter;
        var searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        var acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        var acJSON = isc.JSON.encode(acFlattened);
        if (acJSON.length > minAdvancedCriteriaLength)
            filterMap.set("_advancedCriteria", acJSON);
        var simpleCriteria = {};
        filterMap.forEach(function (value, key) {
            simpleCriteria[key] = value;
        });
        var windowAppViewGrid = window.appViewGrid;
        var appViewGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria, function (aDSResponse, aData, aDSRequest) {
            RediSearchJson.prototype.fetchFacetsCallback();
        });
    };
    RediSearchJson.prototype.createSearchSection = function () {
        var windowAppContext = window._appContext_;
        var searchForm = isc.DynamicForm.create({
            ID: "searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
            items: [{
                    type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon: true,
                    keyPress: function (item, form, keyName, characterValue) {
                        if (keyName == "Enter")
                            RediSearchJson.prototype.executeAppViewGridSearch();
                        return true;
                    },
                    icons: [{
                            name: "clear", src: "[SKIN]actions/close.png", width: 10, height: 10, inline: true, inlineIconAlign: "right", prompt: "Clear Field Contents",
                            click: function (form, item, icon) {
                                item.clearValue();
                                item.focusInItem();
                            }
                        }]
                }]
        });
        var searchFilter = isc.FilterBuilder.create({
            ID: "searchFilter", width: 500, height: 150, autoDraw: false,
            dataSource: windowAppContext.getAppViewDS(), topOperatorAppearance: "none",
            showSubClauseButton: false, criteria: {}
        });
        var fbSearchButton = isc.Button.create({
            ID: "fbSearchButton", title: "Search", autoFit: true, autoDraw: false,
            click: function () {
                RediSearchJson.prototype.executeAppViewGridSearch();
            }
        });
        var fbApplyButton = isc.Button.create({
            ID: "fbApplyButton", title: "Apply", autoFit: true, autoDraw: false,
            click: function () {
                fbWindow.hide();
            }
        });
        var fbResetButton = isc.Button.create({
            ID: "fbResetButton", title: "Reset", autoFit: true, autoDraw: false,
            click: function () {
                searchFilter.clearCriteria();
            }
        });
        var fbCancelButton = isc.IButton.create({
            ID: "fbCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                fbWindow.hide();
            }
        });
        var fbButtonLayout = isc.HStack.create({
            ID: "fbButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [fbSearchButton, fbApplyButton, fbResetButton, fbCancelButton]
        });
        var fbFormLayout = isc.VStack.create({
            ID: "fbFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [searchFilter, fbButtonLayout]
        });
        var fbWindow = isc.Window.create({
            ID: "fbWindow", title: "Filter Builder Window", autoSize: true, autoCenter: true,
            isModal: false, showModalMask: false, autoDraw: false,
            items: [fbFormLayout]
        });
        var tsAdvancedSearch = isc.ToolStripButton.create({
            ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false,
            click: function () {
                fbWindow.show();
            }
        });
        var tsHighlightSearch = isc.ToolStripButton.create({
            ID: "tsHighlightSearch", icon: "[SKIN]/actions/ai-highlight-off-icon.png", prompt: "Highlight matches", showDown: false, actionType: "checkbox", showFocusOutline: false, autoDraw: false,
            click: function () {
                if (tsHighlightSearch.isSelected()) {
                    tsHighlightSearch.setBackgroundColor("white");
                    if (!windowAppContext.isHighlightsAssigned()) {
                        var windowAppViewGrid = window.appViewGrid;
                        var appViewGrid = void 0;
                        appViewGrid = windowAppViewGrid;
                        var highlightColor = windowAppContext.getHighlightFontColor();
                        var dataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
                        var fieldNames = dataSource.getFieldNames(false);
                        var highlightArray = [];
                        var _loop_2 = function (i) {
                            var highlightMap = new Map();
                            highlightMap.set("fieldName", fieldNames[i]);
                            highlightMap.set("textColor", highlightColor);
                            highlightMap.set("cssText", "color:" + highlightColor + ";");
                            highlightMap.set("id", i);
                            var highlightField = {};
                            highlightMap.forEach(function (value, key) {
                                highlightField[key] = value;
                            });
                            highlightArray.add(highlightField);
                        };
                        for (var i = 0; i < fieldNames.length; i++) {
                            _loop_2(i);
                        }
                        appViewGrid.setHilites(highlightArray);
                        windowAppContext.setHighlightsAssigned(true);
                    }
                }
            }
        });
        var tsExecuteSearch = isc.ToolStripButton.create({
            ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false,
            click: function () {
                RediSearchJson.prototype.executeAppViewGridSearch();
            }
        });
        var tsClearSearch = isc.ToolStripButton.create({
            ID: "tsSearchClear", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Criteria", showDown: false, autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var windowSuggestForm = window.suggestForm;
                var suggestForm;
                suggestForm = windowSuggestForm;
                searchFilter.clearCriteria();
                searchForm.clearValues();
                suggestForm.clearValues();
                tsHighlightSearch.deselect();
                appViewGrid.invalidateCache();
                RediSearchJson.prototype.resetFacetList();
                RediSearchJson.prototype.executeAppViewGridSearch();
            }
        });
        var tsSearch;
        if (windowAppContext.isFacetUIEnabled()) {
            tsSearch = isc.ToolStrip.create({
                ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
                members: [tsAdvancedSearch, tsExecuteSearch, tsClearSearch]
            });
        }
        else {
            tsSearch = isc.ToolStrip.create({
                ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
                members: [tsAdvancedSearch, tsHighlightSearch, tsExecuteSearch, tsClearSearch]
            });
        }
        var criteriaSearchLayout = isc.HStack.create({
            ID: "criteriaSearchLayout", width: "100%", align: "center",
            membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
            members: [searchForm, tsSearch]
        });
        var suggestForm = isc.DynamicForm.create({
            ID: "suggestForm", autoDraw: false,
            items: [{
                    name: "_suggest", title: "Suggestions", width: 300,
                    editorType: "ComboBoxItem", optionDataSource: "RSJ-SuggestList",
                    pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                    keyPress: function (item, form, keyName, characterValue) {
                        if (keyName == "Enter") {
                            var windowAppContext_1 = window._appContext_;
                            var windowAppViewGrid = window.appViewGrid;
                            var appViewGrid = void 0;
                            appViewGrid = windowAppViewGrid;
                            var filterMap = new Map();
                            filterMap.set("_dsTitle", windowAppContext_1.getAppViewTitle());
                            filterMap.set("_dsStructure", windowAppContext_1.getDSStructure());
                            filterMap.set("_appPrefix", windowAppContext_1.getPrefix());
                            filterMap.set("_fetchPolicy", windowAppContext_1.getFetchPolicy());
                            filterMap.set("_offset", windowAppContext_1.getCriteriaOffset());
                            filterMap.set("_limit", 10);
                            var suggestTerm = form.getValue("_suggest");
                            filterMap.set("_search", suggestTerm);
                            var simpleCriteria_2 = {};
                            filterMap.forEach(function (value, key) {
                                simpleCriteria_2[key] = value;
                            });
                            appViewGrid.filterData(simpleCriteria_2);
                            RediSearchJson.prototype.resetFacetList();
                        }
                        return true;
                    }
                }]
        });
        var tsClearSuggestion = isc.ToolStripButton.create({
            ID: "tsClearSuggestion", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Suggestion", showDown: false, autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var windowSuggestForm = window.suggestForm;
                var suggestForm;
                suggestForm = windowSuggestForm;
                searchFilter.clearCriteria();
                searchForm.clearValues();
                suggestForm.clearValues();
                tsHighlightSearch.deselect();
                appViewGrid.invalidateCache();
                RediSearchJson.prototype.resetFacetList();
                RediSearchJson.prototype.executeAppViewGridSearch();
            }
        });
        var tsSuggest = isc.ToolStrip.create({
            ID: "tsSuggest", border: "0px", backgroundColor: "white", autoDraw: false,
            members: [tsClearSuggestion]
        });
        var suggestedSearchLayout = isc.HStack.create({
            ID: "suggestedSearchLayout", width: "100%", align: "center",
            membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
            members: [suggestForm, tsSuggest]
        });
        var searchOptionsStack = isc.SectionStack.create({
            ID: "searchOptionsStack", visibilityMode: "mutex", width: "100%", headerHeight: 23, autoDraw: false,
            sections: [
                { title: "Criteria Search", expanded: true, canCollapse: true, items: [criteriaSearchLayout] },
                { title: "Suggested Search", expanded: false, canCollapse: true, items: [suggestedSearchLayout] }
            ]
        });
        return searchOptionsStack;
    };
    RediSearchJson.prototype.deleteSelectedAppViewGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowAppViewGrid = window.appViewGrid;
            var appViewGrid = void 0;
            appViewGrid = windowAppViewGrid;
            if (appViewGrid != null) {
                appViewGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RediSearchJson.prototype.deleteSelectedDocumentsGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowDocumentsGrid = window.documentsGrid;
            var documentsGrid = void 0;
            documentsGrid = windowDocumentsGrid;
            if (documentsGrid != null) {
                documentsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RediSearchJson.prototype.deleteSelectedFlatAppViewGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowDataFlatGrid = window.dataFlatGrid;
            var dataFlatGrid = void 0;
            dataFlatGrid = windowDataFlatGrid;
            if (dataFlatGrid != null) {
                dataFlatGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RediSearchJson.prototype.deleteSelectedHierarchyAppViewGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowDataHierarchyGrid = window.dataHierarchyGrid;
            var dataHierarchyGrid = void 0;
            dataHierarchyGrid = windowDataHierarchyGrid;
            if (dataHierarchyGrid != null) {
                dataHierarchyGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RediSearchJson.prototype.rebuildSearchIndex = function (aResponse) {
        if (aResponse == "OK") {
            var windowAppContext_2 = window._appContext_;
            var windowSchemaGrid = window.schemaGrid;
            var schemaGrid_1;
            schemaGrid_1 = windowSchemaGrid;
            if (schemaGrid_1 != null) {
                schemaGrid_1.removeData(schemaGrid_1.data.get(0));
                isc.Notify.addMessage("Index rebuild initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                setTimeout(function () { schemaGrid_1.invalidateCache(); schemaGrid_1.filterData(RediSearchJson.prototype.defaultCriteria(windowAppContext_2.getDSStructure(), windowAppContext_2.getAppViewTitle())); }, 2000);
            }
        }
    };
    RediSearchJson.prototype.flushDatabase = function (aResponse) {
        if (aResponse == "OK") {
            var windowAppContext = window._appContext_;
            var windowRedisDBInfoWindow = window.redisDBInfoWindow;
            var redisDBInfoWindow = void 0;
            redisDBInfoWindow = windowRedisDBInfoWindow;
            var windowRedisDBInfoForm = window.redisDBInfoForm;
            var redisDBInfoForm = void 0;
            redisDBInfoForm = windowRedisDBInfoForm;
            if ((redisDBInfoWindow != null) && (redisDBInfoForm != null)) {
                windowAppContext.assignFormContext(redisDBInfoForm);
                redisDBInfoForm.saveData();
                redisDBInfoWindow.hide();
                isc.Notify.addMessage("Database flush initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                setTimeout(function () { RediSearchJson.prototype.executeAppViewGridSearch(); }, 2000);
            }
        }
    };
    RediSearchJson.prototype.updateCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    };
    RediSearchJson.prototype.uploadCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Upload complete", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    };
    RediSearchJson.prototype.dafNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataFlatForm = window.dafcUploadForm;
        var dafcUploadForm;
        dafcUploadForm = windowDataFlatForm;
        var windowDataFlatXForm = window.dafxUploadForm;
        var dafxUploadForm;
        dafxUploadForm = windowDataFlatXForm;
        dafxUploadForm.setValue("document_title", dafcUploadForm.getValue("document_title"));
        dafxUploadForm.setValue("document_description", dafcUploadForm.getValue("document_description"));
        dafxUploadForm.saveData("RediSearchJson.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    RediSearchJson.prototype.graphNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataHierarchyNodeForm = window.graphnUploadForm;
        var graphnUploadForm;
        graphnUploadForm = windowDataHierarchyNodeForm;
        var windowDataHierarchyEdgeForm = window.grapheUploadForm;
        var grapheUploadForm;
        grapheUploadForm = windowDataHierarchyEdgeForm;
        grapheUploadForm.setValue("document_title", graphnUploadForm.getValue("document_title"));
        grapheUploadForm.setValue("document_description", graphnUploadForm.getValue("document_description"));
        grapheUploadForm.saveData("RediSearchJson.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    RediSearchJson.prototype.createCommandToolStrip = function () {
        var windowAppContext = window._appContext_;
        var redisDBInfoForm = isc.DynamicForm.create({
            ID: "redisDBInfoForm", width: 400, height: 400, autoDraw: false, dataSource: "RSJ-Database", autoFetchData: false, canEdit: false
        });
        var redisDBInfoLayout = isc.VStack.create({
            ID: "redisDBInfoLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
            layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
            members: [redisDBInfoForm]
        });
        var redisDBInfoWindow = isc.Window.create({
            ID: "redisDBInfoWindow", title: "Redis DB Info Window", width: 410, height: 520, autoCenter: true,
            isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
            items: [redisDBInfoLayout]
        });
        var docUploadForm = isc.DynamicForm.create({
            ID: "docUploadForm", width: 275, height: 75, autoDraw: false,
            dataSource: "RSJ-DocumentGrid",
            fields: [
                { name: "document_title", title: "Title", type: "text", required: true },
                { name: "document_description", title: "Description", type: "text", defaultValue: "None", required: true },
                { name: "document_file", title: "File", type: "binary", required: true }
            ]
        });
        var dufSaveButton = isc.Button.create({
            ID: "dufSaveButton", title: "Upload", autoFit: true, autoDraw: false,
            click: function () {
                if (docUploadForm.valuesAreValid(false, false)) {
                    docUploadForm.saveData("RediSearchJson.prototype.uploadCallback(dsResponse,data,dsRequest)");
                    dufWindow.hide();
                }
                else
                    docUploadForm.validate(false);
            }
        });
        var dufCancelButton = isc.IButton.create({
            ID: "dufCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                dufWindow.hide();
            }
        });
        var dufButtonLayout = isc.HStack.create({
            ID: "dufButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [dufSaveButton, dufCancelButton]
        });
        var docUploadFormLayout = isc.VStack.create({
            ID: "docUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [docUploadForm, dufButtonLayout]
        });
        var dufWindow = isc.Window.create({
            ID: "dufWindow", title: "Document Upload Form Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [docUploadFormLayout]
        });
        var documentsGrid = isc.ListGrid.create({
            ID: "documentsGrid", width: 700, height: 300, autoDraw: false, dataSource: "RSJ-DocumentGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true,
            wrapCells: true, cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
            fields: [
                { name: "document_name", title: "Name" },
                { name: "document_title", title: "Title" },
                { name: "document_type", title: "Type" },
                { name: "document_description", title: "Description" },
                { name: "document_date", title: "Upload Date" },
                { name: "document_size", title: "File Size" }
            ]
        });
        var dgAddButton = isc.Button.create({
            ID: "dgAddButton", title: "Add", autoFit: true, autoDraw: false, disabled: true,
            click: function () {
                isc.say("Add document button pressed");
            }
        });
        var dgDeleteButton = isc.Button.create({
            ID: "dgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = documentsGrid.getSelectedRecord();
                if (lgRecord != null) {
                    isc.confirm("Proceed with row deletion operation?", "RediSearchJson.prototype.deleteSelectedDocumentsGridRow(value ? 'OK' : 'Cancel')");
                }
                else
                    isc.say("You must select a row on the grid to remove.");
            }
        });
        var dgCloseButton = isc.IButton.create({
            ID: "dgCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                dgWindow.hide();
            }
        });
        var dgButtonLayout = isc.HStack.create({
            ID: "dgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [dgAddButton, dgDeleteButton, dgCloseButton]
        });
        var dgFormLayout = isc.VStack.create({
            ID: "dgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [documentsGrid, dgButtonLayout]
        });
        var dgWindow = isc.Window.create({
            ID: "dgWindow", title: "Documents Manager Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [dgFormLayout]
        });
        var fileMenu = isc.Menu.create({
            ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
            data: [
                { title: "Document ...", icon: "[SKIN]/actions/ai-document-commands-icon.png",
                    submenu: [
                        { title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-document-upload-icon.png", click: function () {
                                docUploadForm.clearValues();
                                dufWindow.show();
                            } },
                        { title: "Manage", enabled: true, icon: "[SKIN]/actions/ai-document-manage-icon.png", click: function () {
                                documentsGrid.deselectAllRecords();
                                dgWindow.show();
                            } }
                    ] },
                { isSeparator: true },
                { title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                    submenu: [
                        { title: "Information", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: false, click: function () {
                                var windowAppContext = window._appContext_;
                                windowAppContext.assignFormContext(redisDBInfoForm);
                                redisDBInfoForm.fetchData(RediSearchJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                });
                            } },
                        { title: "Flush DB", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: true, click: function () {
                                var windowAppContext = window._appContext_;
                                windowAppContext.assignFormContext(redisDBInfoForm);
                                redisDBInfoForm.fetchData(RediSearchJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                    isc.confirm("Are you sure you want to flush all data?", "RediSearchJson.prototype.flushDatabase(value ? 'OK' : 'Cancel')");
                                });
                            } }
                    ] },
                { isSeparator: true },
                { title: "Export Data ...", icon: "[SKIN]/actions/ai-export-icon.png", enabled: true,
                    submenu: [
                        { title: "Grid as PDF", icon: "[SKIN]/actions/ai-export-grid-pdf-icon.png", click: function () {
                                var windowAppViewGrid = window.appViewGrid;
                                var appViewGrid;
                                appViewGrid = windowAppViewGrid;
                                isc.Canvas.showPrintPreview(appViewGrid);
                            } },
                        { title: "Grid as CSV", icon: "[SKIN]/actions/ai-export-grid-csv-icon.png", click: function () {
                                var windowAppContext = window._appContext_;
                                RediSearchJson.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                            } },
                        { title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function () {
                                RediSearchJson.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                            } },
                        { title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function () {
                                RediSearchJson.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                            } },
                        { title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png", click: function () {
                                RediSearchJson.prototype.executeAppViewGridExport("command_export_txt", "txt", 100);
                            } }
                    ] }
            ]
        });
        var fileMenuButton = isc.ToolStripMenuButton.create({
            ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
        });
        var schemaGrid;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        if (scDataSource.getFieldNames(false).length > 18) {
            schemaGrid = isc.ListGrid.create({
                ID: "schemaGrid", width: 710, height: 500, autoDraw: false, dataSource: "RSJ-SchemaGrid",
                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                autoFetchData: true, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                listEndEditAction: "next", autoSaveEdits: false,
                getCellCSSText: function (record, rowNum, colNum) {
                    if (colNum == 0)
                        return "font-weight:bold; color:#000000;";
                }
            });
        }
        else {
            schemaGrid = isc.ListGrid.create({
                ID: "schemaGrid", width: 710, height: 300, autoDraw: false, dataSource: "RSJ-SchemaGrid",
                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                autoFetchData: true, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                listEndEditAction: "next", autoSaveEdits: false,
                getCellCSSText: function (record, rowNum, colNum) {
                    if (colNum == 0)
                        return "font-weight:bold; color:#000000;";
                }
            });
        }
        var sgApplyButton = isc.Button.create({
            ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false,
            click: function () {
                schemaGrid.saveAllEdits();
                isc.Notify.addMessage("Updates saved", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        });
        var sgDiscardButton = isc.Button.create({
            ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
            click: function () {
                schemaGrid.discardAllEdits();
                isc.Notify.addMessage("Updates discarded", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        });
        var sgRebuildButton = isc.Button.create({
            ID: "sgRebuildButton", title: "Rebuild", autoFit: true, autoDraw: false,
            click: function () {
                isc.confirm("Rebuilding index will reload all documents - proceed with index rebuild operation?", "RediSearchJson.prototype.rebuildSearchIndex(value ? 'OK' : 'Cancel')");
            }
        });
        var sgCloseButton = isc.IButton.create({
            ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                sgWindow.hide();
            }
        });
        var sgButtonLayout = isc.HStack.create({
            ID: "sgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [sgApplyButton, sgRebuildButton, sgDiscardButton, sgCloseButton]
        });
        var sgFormLayout = isc.VStack.create({
            ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [schemaGrid, sgButtonLayout]
        });
        var sgWindow = isc.Window.create({
            ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [sgFormLayout]
        });
        var tsSchemaButton = isc.ToolStripButton.create({
            ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
            click: function () {
                schemaGrid.invalidateCache();
                schemaGrid.filterData(RediSearchJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                sgWindow.show();
            }
        });
        var appViewForm;
        if (scDataSource.getFieldNames(false).length > 50) {
            appViewForm = isc.DynamicForm.create({
                ID: "appViewForm", width: 600, height: 500, numCols: 8, autoDraw: false,
                dataSource: windowAppContext.getAppViewDS()
            });
        }
        else if (scDataSource.getFieldNames(false).length > 30) {
            appViewForm = isc.DynamicForm.create({
                ID: "appViewForm", width: 600, height: 400, numCols: 6, autoDraw: false,
                dataSource: windowAppContext.getAppViewDS()
            });
        }
        else if (scDataSource.getFieldNames(false).length > 18) {
            appViewForm = isc.DynamicForm.create({
                ID: "appViewForm", width: 600, height: 300, numCols: 4, autoDraw: false,
                dataSource: windowAppContext.getAppViewDS()
            });
        }
        else {
            appViewForm = isc.DynamicForm.create({
                ID: "appViewForm", width: 300, height: 300, numCols: 2, autoDraw: false,
                dataSource: windowAppContext.getAppViewDS()
            });
        }
        var avfSaveButton = isc.Button.create({
            ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                if (appViewForm.valuesAreValid(false, false)) {
                    windowAppContext.assignFormContext(appViewForm);
                    appViewForm.saveData("RediSearchJson.prototype.updateCallback(dsResponse,data,dsRequest)");
                    avfWindow.hide();
                }
                else
                    appViewForm.validate(false);
            }
        });
        var avfCancelButton = isc.IButton.create({
            ID: "avfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                avfWindow.hide();
            }
        });
        var avfButtonLayout = isc.HStack.create({
            ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, membersMargin: 40,
            members: [avfSaveButton, avfCancelButton]
        });
        var appViewFormLayout = isc.VStack.create({
            ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [appViewForm, avfButtonLayout]
        });
        var avfWindow = isc.Window.create({
            ID: "avfWindow", title: "Search Form Window", autoSize: true, canDragResize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [appViewFormLayout]
        });
        var tsAddButton = isc.ToolStripButton.create({
            ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var lgRecord = appViewGrid.getSelectedRecord();
                if (lgRecord == null) {
                    avfWindow.setTitle("Add Grid Form Window");
                    appViewForm.editNewRecord();
                }
                else {
                    var primaryFieldName = "";
                    var primaryDSField = scDataSource.getPrimaryKeyField();
                    if (primaryDSField != null)
                        primaryFieldName = primaryDSField.name;
                    var keyValueMap = new Map();
                    for (var _i = 0, _a = Object.entries(lgRecord); _i < _a.length; _i++) {
                        var _b = _a[_i], k = _b[0], v = _b[1];
                        if (k == primaryFieldName)
                            keyValueMap.set(k, "");
                        else
                            keyValueMap.set(k, v);
                    }
                    var defaultValues_1 = {};
                    keyValueMap.forEach(function (value, key) {
                        defaultValues_1[key] = value;
                    });
                    appViewForm.editNewRecord(defaultValues_1);
                    avfWindow.setTitle("Add (Duplicate) Search Form Window");
                }
                appViewForm.clearErrors(false);
                avfWindow.show();
            }
        });
        var tsEditButton = isc.ToolStripButton.create({
            ID: "tsEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                if (appViewGrid != null) {
                    var lgRecord = appViewGrid.getSelectedRecord();
                    if (lgRecord != null) {
                        appViewForm.clearValues();
                        appViewForm.clearErrors(false);
                        appViewForm.editSelectedData(appViewGrid);
                        avfWindow.setTitle("Edit Search Form Window");
                        avfWindow.show();
                    }
                    else
                        isc.say("You must select a row on the grid to edit.");
                }
            }
        });
        var tsDeleteButton = isc.ToolStripButton.create({
            ID: "tsDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                if (appViewGrid != null) {
                    var lgRecord = appViewGrid.getSelectedRecord();
                    if (lgRecord != null) {
                        isc.confirm("Proceed with row deletion operation?", "RediSearchJson.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                    }
                    else
                        isc.say("You must select a row on the grid to remove.");
                }
            }
        });
        var tsViewButton = isc.ToolStripButton.create({
            ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
            click: function () { isc.say('Document viewing is not enabled for this configuration.'); }
        });
        var detailViewer = isc.DetailViewer.create({
            ID: "detailViewer", width: 400, height: 400, autoDraw: false, dataSource: windowAppContext.getAppViewDS(), showDetailFields: true
        });
        var detailLayout = isc.VStack.create({
            ID: "detailLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
            layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
            members: [detailViewer]
        });
        var detailWindow = isc.Window.create({
            ID: "detailWindow", title: "Detail Window", width: 465, height: 550, autoCenter: true,
            isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
            items: [detailLayout]
        });
        var tsDetailsButton = isc.ToolStripButton.create({
            ID: "tsDetailsButton", icon: "[SKIN]/actions/ai-details-icon.png", prompt: "Row Details", autoDraw: false, showDown: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var lgRecord = appViewGrid.getSelectedRecord();
                if (lgRecord != null) {
                    detailViewer.viewSelectedData(appViewGrid);
                    detailWindow.show();
                }
                else
                    isc.say("You must select a row on the grid to detail.");
            }
        });
        var tsMapButton = isc.ToolStripButton.create({
            ID: "tsMapButton", icon: "[SKIN]/actions/ai-maps-icon.png", prompt: "Show Map", autoDraw: false,
            click: function () { isc.say('Schema is missing GEO fields.'); }
        });
        var tsApplicationGridButton = isc.ToolStripButton.create({
            ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
            click: function () {
                var windowCommandGrid = window.commandGrid;
                var windowAppLayout = window.appLayout;
                var windowAppViewGridLayout = window.appViewGridLayout;
                var commandGrid;
                commandGrid = windowCommandGrid;
                var appViewGridLayout;
                appViewGridLayout = windowAppViewGridLayout;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.showMember(appViewGridLayout);
                appLayout.hideMember(commandGrid);
            }
        });
        var tsCommandGridButton = isc.ToolStripButton.create({
            ID: "tsCommandGridButton", icon: "[SKIN]/actions/ai-command-list-icon.png", prompt: "Command Grid", autoDraw: false,
            click: function () {
                var windowAppContext = window._appContext_;
                var windowCommandGrid = window.commandGrid;
                var windowAppLayout = window.appLayout;
                var windowAppViewGridLayout = window.appViewGridLayout;
                var commandGrid;
                commandGrid = windowCommandGrid;
                var appViewGridLayout;
                appViewGridLayout = windowAppViewGridLayout;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.hideMember(appViewGridLayout);
                appLayout.showMember(commandGrid);
                var filterMap = new Map();
                filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                filterMap.set("_appPrefix", windowAppContext.getPrefix());
                filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                filterMap.set("_limit", windowAppContext.getCriteriaLimit());
                filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());
                filterMap.set("_action", "reload");
                var simpleCriteria = {};
                filterMap.forEach(function (value, key) {
                    simpleCriteria[key] = value;
                });
                commandGrid.invalidateCache();
                commandGrid.filterData(simpleCriteria);
            }
        });
        var tsRedisInsightButton = isc.ToolStripButton.create({
            ID: "tsRedisInsightButton", icon: "[SKIN]/actions/ai-redis-insight-icon.png", prompt: "Redis Insight", autoDraw: false,
            click: function () {
                window.open(windowAppContext.getRedisInsightURL(), "_blank");
            }
        });
        var setGeneralForm = isc.DynamicForm.create({
            ID: "setGeneralForm", autoDraw: false, width: 500, colWidths: [190, "*"],
            fields: [
                { name: "app_group", title: "App Group", type: "text", value: windowAppContext.getGroupName(), canEdit: true, required: true, hint: "Application group", wrapHintText: false },
                { name: "app_name", title: "App Name", type: "text", value: windowAppContext.getAppName(), canEdit: true, required: true, hint: "Application name", wrapHintText: false },
                { name: "ds_name", title: "DS Name", type: "text", value: windowAppContext.getAppViewDS(), canEdit: false, hint: "Data source name", wrapHintText: false },
                { name: "ds_title", title: "DS Title", type: "text", value: windowAppContext.getAppViewTitle(), canEdit: false, required: true, hint: "Data source title", wrapHintText: false }
            ]
        });
        var setGridForm = isc.DynamicForm.create({
            ID: "setGridForm", autoDraw: false, width: 500, colWidths: [190, "*"], isGroup: true, groupTitle: "Grid Options",
            fields: [
                { name: "fetch_policy", title: "Fetch Policy", type: "SelectItem", hint: "Page navigation", canEdit: false, wrapHintText: false, defaultValue: "Virtual Paging", valueMap: { "virtual": "Virtual Paging", "paging": "Single Paging" } },
                { name: "page_size", title: "Page Size", editorType: "SpinnerItem", writeStackedIcons: true, canEdit: false, hint: "Rows per page", wrapHintText: false, defaultValue: 50, min: 19, max: 100, step: 10 },
                { name: "column_filtering", title: "Column Filtering", type: "radioGroup", defaultValue: "Disabled", valueMap: ["Disabled", "Enabled"], vertical: false },
                { name: "csv_header", title: "CSV Header", type: "radioGroup", defaultValue: "Title", valueMap: ["Title", "Field/Type/Title"], vertical: false },
                { name: "highlight_color", title: "Highlight Color", type: "color", defaultValue: windowAppContext.getHighlightFontColor() },
                { name: "facet_count", title: "Facet Count", editorType: "SpinnerItem", writeStackedIcons: false, defaultValue: 10, min: 3, max: 20, step: 1 }
            ]
        });
        var settingsSaveButton = isc.Button.create({
            ID: "settingsSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                if (setGeneralForm.valuesAreValid(false, false)) {
                    var windowAppContext_3 = window._appContext_;
                    windowAppContext_3.setGroupName(setGeneralForm.getValue("app_group"));
                    windowAppContext_3.setAppName(setGeneralForm.getValue("app_name"));
                    var windowHeaderHTMLFlow = window.headerHTMLFlow;
                    var headerHTMLFlow = void 0;
                    headerHTMLFlow = windowHeaderHTMLFlow;
                    headerHTMLFlow.setContents(RediSearchJson.prototype.createHTMLHeader());
                    headerHTMLFlow.redraw();
                    var windowAppViewGrid = window.appViewGrid;
                    var appViewGrid = void 0;
                    appViewGrid = windowAppViewGrid;
                    windowAppContext_3.setFacetValueCount(setGridForm.getValue("facet_count"));
                    if (setGridForm.getValue("column_filtering") == "Enabled")
                        appViewGrid.setShowFilterEditor(true);
                    else
                        appViewGrid.setShowFilterEditor(false);
                    if (setGridForm.getValue("csv_header") == "Title")
                        windowAppContext_3.setGridCSVHeader("title");
                    else
                        windowAppContext_3.setGridCSVHeader("field");
                    var curHighlightColor = windowAppContext_3.getHighlightFontColor();
                    var newHighlightColor = setGridForm.getValue("highlight_color");
                    if (curHighlightColor != newHighlightColor) {
                        windowAppContext_3.setHighlightFontColor(newHighlightColor);
                        windowAppContext_3.setHighlightsAssigned(false);
                    }
                    settingsWindow.hide();
                }
                else
                    setGeneralForm.validate(false);
            }
        });
        var settingsCancelButton = isc.IButton.create({
            ID: "settingsCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                settingsWindow.hide();
            }
        });
        var settingsButtonLayout = isc.HStack.create({
            ID: "settingsButtonLayout", width: "100%", height: 24,
            layoutAlign: "center", autoDraw: false, membersMargin: 40,
            members: [settingsSaveButton, settingsCancelButton]
        });
        var settingsFormLayout = isc.VStack.create({
            ID: "settingsFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [setGeneralForm, setGridForm, settingsButtonLayout]
        });
        var settingsWindow = isc.Window.create({
            ID: "settingsWindow", title: "Settings Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [settingsFormLayout]
        });
        var tsSettingsButton = isc.ToolStripButton.create({
            ID: "tsSettingsButton", icon: "[SKIN]/actions/ai-settings-gear-black-icon.png", prompt: "Settings", autoDraw: false,
            click: function () {
                var windowGridFirstPage = window.gridFirstPage;
                var gridFirstPage;
                gridFirstPage = windowGridFirstPage;
                setGeneralForm.clearErrors(false);
                settingsWindow.show();
            }
        });
        var tsHelpButton = isc.ToolStripButton.create({
            ID: "tsHelpButton", icon: "[SKIN]/actions/ai-help-icon.png", prompt: "Online Help", autoDraw: false,
            click: function () {
                var offset = window.location.href.lastIndexOf("/") + 1;
                var urlBase = window.location.href.substring(0, offset);
                var urlHelpDoc = urlBase + "doc/UG-RedisAppStudio.pdf";
                window.open(urlHelpDoc, "_blank");
            }
        });
        var commandToolStrip = isc.ToolStrip.create({
            ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
            members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsViewButton, tsMapButton, "separator", "starSpacer", tsApplicationGridButton, tsCommandGridButton, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
        });
        return commandToolStrip;
    };
    RediSearchJson.prototype.createAppViewGridLayout = function () {
        var windowAppContext = window._appContext_;
        var CustomListGrid = isc.defineClass("CustomListGrid", "ListGrid").addProperties({
            init: function () {
                this.Super("init", arguments);
                var toolStrip = isc.ToolStrip.create({
                    membersMargin: 5, autoDraw: false,
                    members: [
                        isc.Label.create({
                            ID: "gridPosition",
                            wrap: false, padding: 5, autoDraw: false,
                            contents: "0 to 0 of 0",
                            getRowRangeText: function (arrayVisibleRows, totalRows, lengthIsKnown) {
                                if (!lengthIsKnown)
                                    return "Loading...";
                                else if (arrayVisibleRows[0] != -1) {
                                    var adjTotalRows = totalRows - 1;
                                    return isc.NumberUtil.format((arrayVisibleRows[0] + 1), "#,##0") + " to " + isc.NumberUtil.format((arrayVisibleRows[1]), "#,##0") + " of " + isc.NumberUtil.format(adjTotalRows, "#,##0");
                                }
                                else
                                    return "0 to 0 of 0";
                            }
                        }),
                        isc.LayoutSpacer.create({ width: "*" }),
                        "separator",
                        isc.ImgButton.create({
                            grid: this, src: "[SKIN]/actions/refresh.png", showRollOver: false,
                            prompt: "Refresh", width: 16, height: 16, showDown: false, autoDraw: false,
                            click: function () {
                                RediSearchJson.prototype.executeAppViewGridSearch();
                            }
                        })
                    ]
                });
                this.setProperty("gridComponents", ["filterEditor", "header", "body", "summaryRow", toolStrip]);
            },
            initWidget: function () {
                this.Super("initWidget", arguments);
                this.observe(this, "dataChanged", function () {
                    this.updateRowRangeDisplay();
                });
                this.observe(this, "scrolled", function () {
                    this.updateRowRangeDisplay();
                });
            },
            updateRowRangeDisplay: function () {
                var label = this.gridComponents[4].getMember(0);
                label.setContents(label.getRowRangeText(this.getVisibleRows(), this.getTotalRows(), this.data.lengthIsKnown()));
            }
        });
        var appFacetGrid = isc.TreeGrid.create({
            ID: "appFacetGrid",
            autoDraw: false, width: "15%", height: windowAppContext.getGridHeightPercentage(), autoFetchData: false, showConnectors: true, showResizeBar: true, useAdvancedCriteria: false,
            data: isc.Tree.create({
                modelType: "parent", nameProperty: "facet_name",
                idField: "id", parentIdField: "parent_id",
                data: [{ id: "1", parent_id: "0", facet_name: "Facet List" }]
            }),
            fields: [
                { name: "facet_name", title: "Filter By Facets" }
            ],
            nodeClick: function (aViewer, aNode, aRecordNumber) {
                var windowAppContext = window._appContext_;
                var windowFacetGrid = window.facetGrid;
                var facetGrid;
                facetGrid = windowFacetGrid;
                var rowCount = facetGrid.getTotalRows();
                if ((rowCount > 0) && (aRecordNumber < rowCount)) {
                    var lgRecord = facetGrid.getRecord(aRecordNumber);
                    var facetCountName = String(lgRecord.facet_name);
                    var offset = facetCountName.lastIndexOf(" (");
                    if (offset > 0) {
                        var facetFieldName = lgRecord.field_name;
                        var facetFieldValue = facetCountName.substring(0, offset);
                        var facetFieldNameValue = facetFieldName + ":" + facetFieldValue;
                        if (windowAppContext.facetFieldExists(facetFieldNameValue))
                            windowAppContext.removeFacetField(facetFieldNameValue);
                        else
                            windowAppContext.addFacetField(facetFieldNameValue);
                        RediSearchJson.prototype.executeAppViewGridSearch();
                    }
                }
            }
        });
        var appViewGrid = CustomListGrid.create({
            ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(), autoDraw: false,
            height: windowAppContext.getGridHeightPercentage(), autoFetchData: true,
            showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: true,
            useAdvancedFieldPicker: true, canEditTitles: true, expansionFieldImageShowSelected: false,
            canExpandRecords: false, canEdit: false, leaveScrollbarGap: false,
            initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
            recordDoubleClick: function () {
                var windowAppViewGrid = window.appViewGrid;
                var windowDetailViewer = window.detailViewer;
                var windowDetailWindow = window.detailWindow;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var detailViewer;
                detailViewer = windowDetailViewer;
                var detailWindow;
                detailWindow = windowDetailWindow;
                var lgRecord = appViewGrid.getSelectedRecord();
                if (lgRecord != null) {
                    detailViewer.viewSelectedData(appViewGrid);
                    detailWindow.show();
                }
                else
                    isc.say("You must select a row on the grid to detail.");
            }
        });
        var appViewGridLayout;
        if (windowAppContext.isFacetUIEnabled()) {
            appViewGrid.setWidth("85%");
            appViewGridLayout = isc.HStack.create({
                ID: "appViewGridLayout", width: "100%", height: "100%", autoDraw: false,
                members: [appFacetGrid, appViewGrid]
            });
        }
        else {
            appViewGrid.setWidth("100%");
            appViewGridLayout = isc.HStack.create({
                ID: "appViewGridLayout", width: "100%", height: "100%", autoDraw: false,
                members: [appViewGrid]
            });
        }
        return appViewGridLayout;
    };
    RediSearchJson.prototype.createCommandGrid = function () {
        var windowAppContext = window._appContext_;
        var commandGrid = isc.ListGrid.create({
            ID: "commandGrid", dataSource: "RSJ-DocCmdGrid", autoDraw: false, width: "100%",
            height: windowAppContext.getGridHeightPercentage(), autoFetchData: false, showFilterEditor: false,
            allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false, canEditTitles: false,
            expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false, leaveScrollbarGap: false,
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            wrapCells: true, cellHeight: 50,
            recordDoubleClick: function () {
                var windowCommandGrid = window.commandGrid;
                var commandGrid;
                commandGrid = windowCommandGrid;
                var lgRecord = commandGrid.getSelectedRecord();
                if (lgRecord != null) {
                    window.open(lgRecord.command_link, "_blank");
                }
                else
                    isc.say("You must select a row on the grid to show help details.");
            },
            getCellCSSText: function (record, rowNum, colNum) {
                if (this.getFieldName(colNum) == "redis_command")
                    return "font-weight:bold; color:#000000;";
                else if (this.getFieldName(colNum) == "redis_parameters")
                    return "font-weight:lighter; font-style: italic; color:#000000;";
            }
        });
        return commandGrid;
    };
    RediSearchJson.prototype.createFacetGrid = function () {
        var facetGrid = isc.ListGrid.create({
            ID: "facetGrid", dataSource: "RSJ-FacetGrid", autoDraw: false, visibility: "hidden", autoFetchData: false
        });
        return facetGrid;
    };
    RediSearchJson.prototype.init = function () {
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", { multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200 });
        var headerSection = this.createHeaderSection();
        var searchSection = this.createSearchSection();
        var commandToolStrip = this.createCommandToolStrip();
        var appViewGridLayout = this.createAppViewGridLayout();
        var commandGrid = this.createCommandGrid();
        this.appLayout = isc.VStack.create({
            ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
            members: [headerSection, searchSection, commandToolStrip, appViewGridLayout, commandGrid]
        });
        this.appLayout.hideMember(commandGrid);
        this.createFacetGrid();
    };
    RediSearchJson.prototype.show = function () {
        this.appLayout.show();
    };
    RediSearchJson.prototype.hide = function () {
        this.appLayout.hide();
    };
    return RediSearchJson;
}());
//# sourceMappingURL=RediSearchJson.js.map