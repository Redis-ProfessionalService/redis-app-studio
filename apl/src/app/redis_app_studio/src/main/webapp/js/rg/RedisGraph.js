var RedisGraph = (function () {
    function RedisGraph(anAppContext) {
        this.appContext = anAppContext;
    }
    RedisGraph.prototype.createHTMLHeader = function () {
        var windowAppContext = window._appContext_;
        var appName;
        if (windowAppContext.isModelerEnabled())
            appName = windowAppContext.getAppName();
        else
            appName = "Application Launcher";
        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis App Studio\" class=\"ahImage\" src=\"images/redis-app-studio.svg\" height=\"99\" width=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + appName + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    };
    RedisGraph.prototype.createHeaderSection = function () {
        var htmlString = this.createHTMLHeader();
        var headerHTMLFlow = isc.HTMLFlow.create({ ID: "headerHTMLFlow", width: "100%", height: "5%", autoDraw: false, contents: htmlString });
        return headerHTMLFlow;
    };
    RedisGraph.prototype.defaultCriteria = function (aDSStructure, aDSTitle, aFetchLimit) {
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
            _limit: fetchLimit,
            _graphCriteriaCount: 0
        };
        return simpleCriteria;
    };
    RedisGraph.prototype.executeAppViewGraphSearch = function () {
        var minAdvancedCriteriaLength = 89;
        var windowAppContext = window._appContext_;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        var windowSearchForm = window.searchForm;
        var searchForm;
        searchForm = windowSearchForm;
        var windowGasGrid = window.gasGrid;
        var gasGrid;
        gasGrid = windowGasGrid;
        var listGridRecord;
        var recordCount = gasGrid.data.getLength();
        filterMap.set("_dgCriterionCount", recordCount);
        for (var recordOffset = 0; recordOffset < recordCount; recordOffset++) {
            listGridRecord = gasGrid.data.get(recordOffset);
            filterMap.set("dgc_" + recordOffset + "_object_type", listGridRecord.object_type);
            filterMap.set("dgc_" + recordOffset + "_object_identifier", listGridRecord.object_identifier);
            filterMap.set("dgc_" + recordOffset + "_hop_count", listGridRecord.hop_count);
            filterMap.set("dgc_" + recordOffset + "_edge_direction", listGridRecord.edge_direction);
            if (listGridRecord.ds_criteria != null) {
                var acFlattened = isc.DataSource.flattenCriteria(listGridRecord.ds_criteria);
                var acJSON = isc.JSON.encode(acFlattened);
                if (acJSON.length > minAdvancedCriteriaLength)
                    filterMap.set("dgc_" + recordOffset + "_ds_criteria", acJSON);
            }
        }
        var searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        var simpleCriteria = {};
        filterMap.forEach(function (value, key) {
            simpleCriteria[key] = value;
        });
        var windowAppViewGrid = window.appViewGrid;
        var appViewGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    };
    RedisGraph.prototype.executeAppViewGridExport = function (anAction, aFormat, aFetchLimit) {
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
    RedisGraph.prototype.executeAppViewGridSearch = function () {
        var minAdvancedCriteriaLength = 89;
        var windowAppContext = window._appContext_;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
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
        appViewGrid.filterData(simpleCriteria);
    };
    RedisGraph.prototype.executeAppViewGridFetch = function () {
        var minAdvancedCriteriaLength = 89;
        var windowAppContext = window._appContext_;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());
        var simpleCriteria = {};
        filterMap.forEach(function (value, key) {
            simpleCriteria[key] = value;
        });
        var windowAppViewGrid = window.appViewGrid;
        var appViewGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    };
    RedisGraph.prototype.createSearchSection = function () {
        var windowAppContext = window._appContext_;
        var searchForm = isc.DynamicForm.create({
            ID: "searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
            items: [{
                    type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon: true,
                    keyPress: function (item, form, keyName, characterValue) {
                        if (keyName == "Enter")
                            RedisGraph.prototype.executeAppViewGraphSearch();
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
        var gasNodeFilter = isc.FilterBuilder.create({
            ID: "gasNodeFilter", width: 500, height: 125, autoDraw: false,
            dataSource: windowAppContext.getAppPrefixDS("NodeFilter"), topOperatorAppearance: "none",
            showSubClauseButton: false, criteria: {}
        });
        var gasnApplyButton = isc.Button.create({
            ID: "gasnApplyButton", title: "Apply", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = gasGrid.getSelectedRecord();
                if (lgRecord != null) {
                    lgRecord.ds_criteria = gasNodeFilter.getCriteria();
                }
                gasnfWindow.hide();
            }
        });
        var gasnResetButton = isc.Button.create({
            ID: "gasnResetButton", title: "Reset", autoFit: true, autoDraw: false,
            click: function () {
                gasNodeFilter.clearCriteria();
            }
        });
        var gasnCancelButton = isc.IButton.create({
            ID: "gasnCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                gasnfWindow.hide();
            }
        });
        var gasnButtonLayout = isc.HStack.create({
            ID: "gasnButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [gasnApplyButton, gasnResetButton, gasnCancelButton]
        });
        var gasnFormLayout = isc.VStack.create({
            ID: "gasnFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [gasNodeFilter, gasnButtonLayout]
        });
        var gasnfWindow = isc.Window.create({
            ID: "gasnfWindow", title: "Node Filter Builder Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [gasnFormLayout]
        });
        var gasRelFilter = isc.FilterBuilder.create({
            ID: "gasRelFilter", width: 500, height: 125, autoDraw: false,
            dataSource: windowAppContext.getAppPrefixDS("RelationshipFilter"), topOperatorAppearance: "none",
            showSubClauseButton: false, criteria: {}
        });
        var gasrApplyButton = isc.Button.create({
            ID: "gasrApplyButton", title: "Apply", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = gasGrid.getSelectedRecord();
                if (lgRecord != null) {
                    lgRecord.ds_criteria = gasRelFilter.getCriteria();
                }
                gasrfWindow.hide();
            }
        });
        var gasrResetButton = isc.Button.create({
            ID: "gasrResetButton", title: "Reset", autoFit: true, autoDraw: false,
            click: function () {
                gasRelFilter.clearCriteria();
            }
        });
        var gasrCancelButton = isc.IButton.create({
            ID: "gasrCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                gasrfWindow.hide();
            }
        });
        var gasrButtonLayout = isc.HStack.create({
            ID: "gasrButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [gasrApplyButton, gasrResetButton, gasrCancelButton]
        });
        var gasrFormLayout = isc.VStack.create({
            ID: "gasrFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [gasRelFilter, gasrButtonLayout]
        });
        var gasrfWindow = isc.Window.create({
            ID: "gasrfWindow", title: "Relationship Filter Builder Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [gasrFormLayout]
        });
        var gasAddButton = isc.ToolStripButton.create({
            ID: "gasAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
            click: function () {
                var rowCount = gasGrid.data.length + 1;
                var isEven = rowCount % 2 == 0;
                if (isEven) {
                    var relIdentifier = void 0;
                    relIdentifier = windowAppContext.getGraphRelTypes().get(0);
                    gasGrid.data.add({ object_type: "Relationship", object_identifier: relIdentifier, hop_count: 0, edge_direction: "None", property_criteria: "Criteria" });
                    gasGrid.setValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                    gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                }
                else {
                    var nodeIdentifier = void 0;
                    nodeIdentifier = windowAppContext.getGraphNodeLabels().get(0);
                    gasGrid.data.add({ object_type: "Node", object_identifier: nodeIdentifier, hop_count: 0, edge_direction: "None", property_criteria: "Criteria" });
                    gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                    gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                }
                gasGrid.startEditing(gasGrid.data.length - 1);
            }
        });
        var gasDeleteButton = isc.ToolStripButton.create({
            ID: "gasDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
            click: function () {
                var lgRecord = gasGrid.getSelectedRecord();
                if (lgRecord != null)
                    gasGrid.data.remove(lgRecord);
            }
        });
        var gasToolStrip = isc.ToolStrip.create({
            ID: "gasToolStrip", width: 500, height: 32, autoDraw: false,
            members: [gasAddButton, "separator", gasDeleteButton]
        });
        var gasGrid = isc.ListGrid.create({
            ID: "gasGrid", width: 500, height: 200, autoDraw: false, autoFetchData: false,
            canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false,
            baseStyle: "alternateGridCell", showHeaderContextMenu: false,
            autoSaveEdits: true, canEdit: true, wrapCells: false,
            editEvent: "doubleClick", listEndEditAction: "next",
            cellClick: function (record, rowNum, colNum) {
                if (colNum == 4) {
                    gasGrid.endEditing();
                    var listGridRecord = void 0;
                    listGridRecord = gasGrid.data.get(rowNum);
                    if (listGridRecord != null) {
                        if (listGridRecord.object_type === "Node") {
                            gasNodeFilter.setCriteria(listGridRecord.ds_criteria);
                            gasnfWindow.show();
                        }
                        else {
                            gasRelFilter.setCriteria(listGridRecord.ds_criteria);
                            gasrfWindow.show();
                        }
                    }
                    else
                        isc.say("listGridRecord is 'null' and cannot show filter form.");
                }
            },
            fields: [
                { name: "object_type", title: "Object", width: 125, valueMap: ["Node", "Relationship"],
                    changed: function (form, item, value) {
                        var windowAppContext = window._appContext_;
                        var lgRecord = gasGrid.getSelectedRecord();
                        if (lgRecord != null) {
                            lgRecord.ds_criteria = {};
                        }
                        if (value === "Relationship") {
                            gasGrid.setValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                            gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphRelTypes());
                        }
                        else {
                            gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                            gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                        }
                    } },
                { name: "object_identifier", title: "Identifier", width: 100, valueMap: windowAppContext.getGraphNodeLabels() },
                { name: "hop_count", title: "Hops", type: "integer", width: 100, align: "center", editorType: "SpinnerItem", minValue: 0, maxValue: 20, defaultValue: 0 },
                { name: "edge_direction", title: "Direction", width: 100, valueMap: ["None", "Outbound", "Inbound"], defaultValue: "None" },
                { name: "property_criteria", title: "Criteria", type: "image", width: 75, align: "center", canEdit: false, imageURLPrefix: "direction/", imageURLSuffix: ".png", defaultValue: "Criteria" },
                { name: "ds_criteria", hidden: true, defaultValue: {} }
            ]
        });
        var gasSearchButton = isc.Button.create({
            ID: "gasSearchButton", title: "Search", autoFit: true, autoDraw: false,
            click: function () {
                gasGrid.endEditing();
                RedisGraph.prototype.executeAppViewGraphSearch();
            }
        });
        var gasApplyButton = isc.Button.create({
            ID: "gasApplyButton", title: "Apply", autoFit: true, autoDraw: false,
            click: function () {
                gasGrid.endEditing();
                gasWindow.hide();
            }
        });
        var gasResetButton = isc.Button.create({
            ID: "gasResetButton", title: "Reset", autoFit: true, autoDraw: false,
            click: function () {
                while (!gasGrid.data.isEmpty())
                    gasGrid.data.removeAt(0);
                gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
            }
        });
        var gasCancelButton = isc.IButton.create({
            ID: "gasCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                gasGrid.discardAllEdits();
                gasWindow.hide();
            }
        });
        var gasButtonLayout = isc.HStack.create({
            ID: "gasButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, layoutTopMargin: 30, membersMargin: 40,
            members: [gasSearchButton, gasApplyButton, gasResetButton, gasCancelButton]
        });
        var gasFormLayout = isc.VStack.create({
            ID: "gasFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30,
            members: [gasToolStrip, gasGrid, gasButtonLayout]
        });
        var gasWindow = isc.Window.create({
            ID: "gasWindow", title: "Graph Advanced Search Window", autoSize: true, autoCenter: true,
            isModal: false, showModalMask: false, autoDraw: false,
            items: [gasFormLayout]
        });
        var tsAdvancedSearch = isc.ToolStripButton.create({
            ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false,
            click: function () {
                gasWindow.show();
            }
        });
        var tsExecuteSearch = isc.ToolStripButton.create({
            ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false,
            click: function () {
                RedisGraph.prototype.executeAppViewGraphSearch();
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
                searchForm.clearValues();
                suggestForm.clearValues();
                var windowGasGrid = window.gasGrid;
                var gasGrid;
                gasGrid = windowGasGrid;
                while (!gasGrid.data.isEmpty())
                    gasGrid.data.removeAt(0);
                gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                gasRelFilter.clearCriteria();
                gasNodeFilter.clearCriteria();
                appViewGrid.invalidateCache();
                appViewGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
            }
        });
        var tsSearch = isc.ToolStrip.create({
            ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
            members: [tsAdvancedSearch, tsExecuteSearch, tsClearSearch]
        });
        var criteriaSearchLayout = isc.HStack.create({
            ID: "criteriaSearchLayout", width: "100%", align: "center",
            membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
            members: [searchForm, tsSearch]
        });
        var suggestForm = isc.DynamicForm.create({
            ID: "suggestForm", autoDraw: false,
            items: [{
                    name: "_suggest", title: "Suggestions", width: 300,
                    editorType: "ComboBoxItem", optionDataSource: "RG-SuggestList",
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
                            var simpleCriteria_1 = {};
                            filterMap.forEach(function (value, key) {
                                simpleCriteria_1[key] = value;
                            });
                            appViewGrid.filterData(simpleCriteria_1);
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
                searchForm.clearValues();
                suggestForm.clearValues();
                var windowGasGrid = window.gasGrid;
                var gasGrid;
                gasGrid = windowGasGrid;
                while (!gasGrid.data.isEmpty())
                    gasGrid.data.removeAt(0);
                gasGrid.setValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                gasGrid.setEditorValueMap("object_identifier", windowAppContext.getGraphNodeLabels());
                gasRelFilter.clearCriteria();
                gasNodeFilter.clearCriteria();
                appViewGrid.invalidateCache();
                appViewGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
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
    RedisGraph.prototype.deleteSelectedAppViewGridRow = function (aResponse) {
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
    RedisGraph.prototype.deleteSelectedApplicationsGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowApplicationsGrid = window.applicationsGrid;
            var applicationsGrid = void 0;
            applicationsGrid = windowApplicationsGrid;
            if (applicationsGrid != null) {
                applicationsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RedisGraph.prototype.deleteSelectedDocumentsGridRow = function (aResponse) {
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
    RedisGraph.prototype.deleteSelectedGraphGridRelRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowGraphGridRel = window.graphGridRel;
            var graphGridRel = void 0;
            graphGridRel = windowGraphGridRel;
            if (graphGridRel != null) {
                graphGridRel.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RedisGraph.prototype.updateCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
        var windowAppContext = window._appContext_;
        if (windowAppContext.isGraphEnabled()) {
            var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
            var windowGraphGridRel = window.graphGridRel;
            var graphGridRel = void 0;
            graphGridRel = windowGraphGridRel;
            var primaryKeyField = scDataSource.getPrimaryKeyField();
            var keyName = primaryKeyField.name;
            var simpleCriteria = {};
            simpleCriteria[keyName] = appViewForm.getValue(keyName);
            graphGridRel.invalidateCache();
            graphGridRel.filterData(simpleCriteria);
        }
    };
    RedisGraph.prototype.flushDatabase = function (aResponse) {
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
                setTimeout(function () { RedisGraph.prototype.executeAppViewGridFetch(); }, 2000);
            }
        }
    };
    RedisGraph.prototype.fieldPrefix = function (aFieldName) {
        var fieldName = aFieldName.toLowerCase();
        var offset = fieldName.indexOf("_");
        if (offset < 2)
            return fieldName;
        else
            return fieldName.substring(0, offset);
    };
    RedisGraph.prototype.uniqueFormNames = function (aDS) {
        var formName;
        var fieldPrefix;
        var fieldLookup;
        var formNames = new Map();
        formNames.set("common", "Common");
        for (var _i = 0, _a = aDS.getFieldNames(false); _i < _a.length; _i++) {
            var fieldName = _a[_i];
            fieldPrefix = this.fieldPrefix(fieldName);
            if (!fieldPrefix.startsWith("ras")) {
                fieldLookup = formNames.get(fieldPrefix);
                if (fieldLookup == undefined) {
                    formName = fieldPrefix.charAt(0).toUpperCase() + fieldPrefix.slice(1);
                    formNames.set(fieldPrefix, formName);
                }
            }
        }
        return formNames;
    };
    RedisGraph.prototype.createGraphRelationshipFormLayout = function () {
        var windowAppContext = window._appContext_;
        var graphEdgeFormLayout = isc.VStack.create({
            ID: "graphEdgeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 2
        });
        var graphRelForm = isc.DynamicForm.create({
            ID: "graphRelForm", width: 300, numCols: 2, autoDraw: false, dataSource: windowAppContext.getAppViewRelDS()
        });
        graphEdgeFormLayout.addMember(graphRelForm);
        var graphEdgeVertexForm = isc.DynamicForm.create({
            ID: "graphEdgeVertexForm", width: 300, numCols: 2, autoDraw: false,
            items: [{
                    name: "_suggest", title: "End Node", editorType: "ComboBoxItem", optionDataSource: "RG-SuggestList", required: true,
                    pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                    keyPress: function (item, form, keyName, characterValue) {
                        if (keyName == "Enter") {
                            var windowGraphRelForm = window.graphRelForm;
                            var graphRelForm_1;
                            graphRelForm_1 = windowGraphRelForm;
                            graphRelForm_1.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                        }
                        return true;
                    }
                }]
        });
        graphEdgeFormLayout.addMember(graphEdgeVertexForm);
        var fiVertexName = graphRelForm.getItem("common_vertex_name");
        fiVertexName.visible = false;
        return graphEdgeFormLayout;
    };
    RedisGraph.prototype.createGraphNodeFormLayout = function (aDS) {
        var uiField;
        var formName;
        var avProperties;
        var dynamicForm;
        var dsField;
        var windowAppContext = window._appContext_;
        var graphNodeFormLayout = isc.VStack.create({
            ID: "graphNodeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 5
        });
        var formNames = this.uniqueFormNames(aDS);
        formNames.forEach(function (value, key) {
            var uiFields = [];
            formName = key + "AppViewForm";
            avProperties = { ID: formName, width: 500, numCols: 2, colWidths: [150, "*"], autoDraw: false, isGroup: true, groupTitle: value };
            for (var _i = 0, _a = aDS.getFieldNames(false); _i < _a.length; _i++) {
                var fieldName = _a[_i];
                if (fieldName.startsWith(key)) {
                    dsField = aDS.getField(fieldName);
                    uiField = {};
                    uiField["name"] = dsField.name;
                    uiField["title"] = dsField.title;
                    if (fieldName.startsWith("common_")) {
                        uiField["required"] = "true";
                        if (fieldName == "common_vertex_label") {
                            uiField["hint"] = "<nobr>Node label</nobr>";
                            uiField["editorType"] = "SelectItem";
                            uiField["changed"] = "RedisGraph.prototype.showGraphNodeForm(value);";
                        }
                        else if (fieldName == "common_name") {
                            uiField["hint"] = "<nobr>Node name</nobr>";
                        }
                    }
                    uiFields.add(uiField);
                }
            }
            avProperties["fields"] = uiFields;
            dynamicForm = isc.DynamicForm.create(avProperties);
            var selectNodeLabels = [];
            formNames.forEach(function (value, key) {
                if ((!key.startsWith("common")) && (!key.startsWith("ras"))) {
                    selectNodeLabels.add(value);
                }
            });
            dynamicForm.setValueMap("common_vertex_label", selectNodeLabels);
            graphNodeFormLayout.addMember(dynamicForm);
            windowAppContext.add(formName, dynamicForm);
        });
        return graphNodeFormLayout;
    };
    RedisGraph.prototype.graphNodeFormsClearValues = function () {
        var windowAppContext = window._appContext_;
        var windowAppViewForm = window.appViewForm;
        var dynamicForm;
        var formName;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        var formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach(function (value, key) {
            formName = key + "AppViewForm";
            dynamicForm = windowAppContext.get(formName);
            dynamicForm.clearValues();
            dynamicForm.clearErrors(false);
            dynamicForm.show();
        });
    };
    RedisGraph.prototype.graphNodeFormsIsValid = function (aShowValid) {
        var windowAppContext = window._appContext_;
        var dynamicForm;
        dynamicForm = windowAppContext.get("commonAppViewForm");
        if (aShowValid)
            dynamicForm.validate(false);
        else if (!dynamicForm.valuesAreValid(false, false))
            return false;
        return true;
    };
    RedisGraph.prototype.appViewToGraphNodeForms = function () {
        var windowAppContext = window._appContext_;
        var windowAppViewForm = window.appViewForm;
        var appViewForm;
        appViewForm = windowAppViewForm;
        var dynamicForm;
        var formName;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        var formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach(function (value, key) {
            formName = key + "AppViewForm";
            dynamicForm = windowAppContext.get(formName);
            dynamicForm.clearValues();
            for (var _i = 0, _a = dynamicForm.fields; _i < _a.length; _i++) {
                var formItem = _a[_i];
                dynamicForm.setValue(formItem.name, appViewForm.getValue(formItem.name));
            }
        });
    };
    RedisGraph.prototype.graphNodesToAppViewForm = function () {
        var windowAppContext = window._appContext_;
        var windowAppViewForm = window.appViewForm;
        var appViewForm;
        appViewForm = windowAppViewForm;
        var dynamicForm;
        var formName;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        var formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach(function (value, key) {
            formName = key + "AppViewForm";
            dynamicForm = windowAppContext.get(formName);
            for (var _i = 0, _a = dynamicForm.fields; _i < _a.length; _i++) {
                var formItem = _a[_i];
                appViewForm.setValue(formItem.name, dynamicForm.getValue(formItem.name));
            }
        });
    };
    RedisGraph.prototype.showGraphNodeForm = function (aLabelName) {
        if (aLabelName != undefined) {
            var windowAppContext_2 = window._appContext_;
            var dynamicForm_1;
            var formName_1;
            var formLabelPrefix = aLabelName.toLowerCase();
            var selectedFormName_1 = formLabelPrefix + "AppViewForm";
            var scDataSource = isc.DataSource.get(windowAppContext_2.getAppViewDS());
            var formNames = this.uniqueFormNames(scDataSource);
            formNames.forEach(function (value, key) {
                formName_1 = key + "AppViewForm";
                dynamicForm_1 = windowAppContext_2.get(formName_1);
                if (formName_1 === selectedFormName_1)
                    dynamicForm_1.show();
                else if (formName_1 != "commonAppViewForm")
                    dynamicForm_1.hide();
            });
        }
    };
    RedisGraph.prototype.graphVisualizationOptionsToURL = function (isDownload) {
        var windowAppContext = window._appContext_;
        var windowGVOptionsForm = window.gvOptionsForm;
        var gvOptionsForm;
        gvOptionsForm = windowGVOptionsForm;
        var gvURL = windowAppContext.getGraphVisualizationURL() + "?";
        gvURL += "is_modeler=false&";
        gvURL += "is_hierarchical=" + gvOptionsForm.getValue("is_hierarchical") + "&";
        gvURL += "is_matched=" + gvOptionsForm.getValue("is_matched") + "&";
        gvURL += "node_shape=" + gvOptionsForm.getValue("node_shape") + "&";
        gvURL += "node_color=" + gvOptionsForm.getValue("node_color") + "&";
        gvURL += "edge_arrow=" + gvOptionsForm.getValue("edge_arrow") + "&";
        gvURL += "edge_color=" + gvOptionsForm.getValue("edge_color") + "&";
        gvURL += "match_color=" + gvOptionsForm.getValue("match_color");
        if (isDownload)
            gvURL += "&is_download=true";
        var gvEncodedURL = gvURL.replace(/#/g, "%23");
        isc.logWarn("gvEncodedURL = " + gvEncodedURL);
        return gvEncodedURL;
    };
    RedisGraph.prototype.createCommandToolStrip = function () {
        var windowAppContext = window._appContext_;
        var redisDBInfoForm = isc.DynamicForm.create({
            ID: "redisDBInfoForm", width: 400, height: 400, autoDraw: false, dataSource: "RG-Database", autoFetchData: false, canEdit: false
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
        var fileMenu = isc.Menu.create({
            ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
            data: [
                { title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                    submenu: [
                        { title: "Information", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: false, click: function () {
                                var windowAppContext = window._appContext_;
                                windowAppContext.assignFormContext(redisDBInfoForm);
                                redisDBInfoForm.fetchData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                });
                            } },
                        { title: "Flush DB", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: true, click: function () {
                                var windowAppContext = window._appContext_;
                                windowAppContext.assignFormContext(redisDBInfoForm);
                                redisDBInfoForm.fetchData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                    isc.confirm("Are you sure you want to flush all data?", "RedisGraph.prototype.flushDatabase(value ? 'OK' : 'Cancel')");
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
                                RedisGraph.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                            } },
                        { title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function () {
                                RedisGraph.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                            } },
                        { title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function () {
                                RedisGraph.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                            } },
                        { title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png", click: function () {
                                RedisGraph.prototype.executeAppViewGridExport("command_export_txt", "txt", 100);
                            } }
                    ] }
            ]
        });
        var fileMenuButton = isc.ToolStripMenuButton.create({
            ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
        });
        var tsSchemaButton;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        var nodeSchemaGrid = isc.ListGrid.create({
            ID: "nodeSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "RG-NodeSchemaGrid",
            initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
            autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            listEndEditAction: "next", autoSaveEdits: false,
            getCellCSSText: function (record, rowNum, colNum) {
                if (colNum == 0)
                    return "font-weight:bold; color:#000000;";
            }
        });
        var relSchemaGrid = isc.ListGrid.create({
            ID: "relSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "RG-RelSchemaGrid",
            initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
            autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            listEndEditAction: "next", autoSaveEdits: false,
            getCellCSSText: function (record, rowNum, colNum) {
                if (colNum == 0)
                    return "font-weight:bold; color:#000000;";
            }
        });
        var tsGraphSchemaTab = isc.TabSet.create({
            ID: "tsGraphSchemaTab", tabBarPosition: "top", width: 725, height: 475, autoDraw: false,
            tabs: [
                { title: "Node Schema", pane: nodeSchemaGrid },
                { title: "Relationship Schema", pane: relSchemaGrid }
            ]
        });
        var sgApplyButton = isc.Button.create({
            ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false,
            click: function () {
                var windowGraphSchemaTab = window.tsGraphSchemaTab;
                var tsGraphSchemaTab;
                tsGraphSchemaTab = windowGraphSchemaTab;
                var selectedTabOffset = tsGraphSchemaTab.getSelectedTabNumber();
                if (selectedTabOffset == 0) {
                    nodeSchemaGrid.saveAllEdits();
                    isc.Notify.addMessage("Node updates saved", null, null, {
                        canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                    });
                }
                else {
                    relSchemaGrid.saveAllEdits();
                    isc.Notify.addMessage("Relationship updates saved", null, null, {
                        canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                    });
                }
            }
        });
        var sgDiscardButton = isc.Button.create({
            ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
            click: function () {
                nodeSchemaGrid.discardAllEdits();
                relSchemaGrid.discardAllEdits();
                isc.Notify.addMessage("Updates discarded", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
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
            members: [sgApplyButton, sgDiscardButton, sgCloseButton]
        });
        var sgFormLayout = isc.VStack.create({
            ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [tsGraphSchemaTab, sgButtonLayout]
        });
        var sgWindow = isc.Window.create({
            ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [sgFormLayout]
        });
        tsSchemaButton = isc.ToolStripButton.create({
            ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
            click: function () {
                nodeSchemaGrid.invalidateCache();
                nodeSchemaGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function (aDSResponse, aData, aDSRequest) {
                    var gridData;
                    gridData = aData;
                    var windowAppViewGrid = window.appViewGrid;
                    var appViewGrid;
                    appViewGrid = windowAppViewGrid;
                    var isVisible;
                    var gridRecord;
                    for (var recOffset = 0; recOffset < gridData.getLength(); recOffset++) {
                        isVisible = false;
                        gridRecord = gridData.get(recOffset);
                        var listGridFields = appViewGrid.getFields();
                        for (var _i = 0, listGridFields_1 = listGridFields; _i < listGridFields_1.length; _i++) {
                            var lgField = listGridFields_1[_i];
                            if (gridRecord.item_name == lgField.name) {
                                isVisible = true;
                                gridRecord.item_title = lgField.title;
                            }
                        }
                        gridRecord.isVisible = isVisible;
                    }
                    relSchemaGrid.invalidateCache();
                    relSchemaGrid.filterData(RedisGraph.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                });
                sgWindow.show();
            }
        });
        var avfWindow;
        var appViewForm;
        var appViewFormLayout;
        appViewForm = isc.DynamicForm.create({
            ID: "appViewForm", autoDraw: false, dataSource: windowAppContext.getAppViewDS()
        });
        var avfSaveButton = isc.Button.create({
            ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                if (RedisGraph.prototype.graphNodeFormsIsValid(false)) {
                    RedisGraph.prototype.graphNodesToAppViewForm();
                    windowAppContext.assignFormContext(appViewForm);
                    appViewForm.saveData("RedisGraph.prototype.updateCallback(dsResponse,data,dsRequest)");
                    avfWindow.hide();
                }
                else
                    RedisGraph.prototype.graphNodeFormsIsValid(true);
            }
        });
        var avfCancelButton = isc.IButton.create({
            ID: "avfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                avfWindow.hide();
            }
        });
        var avfButtonLayout = isc.HStack.create({
            ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 175, membersMargin: 40,
            members: [avfSaveButton, avfCancelButton]
        });
        var graphNodeFormLayout = this.createGraphNodeFormLayout(scDataSource);
        appViewFormLayout = isc.VStack.create({
            ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 10,
            layoutBottomMargin: 10, layoutLeftMargin: 45, layoutRightMargin: 10, membersMargin: 20,
            members: [graphNodeFormLayout, avfButtonLayout]
        });
        var graphEdgeFormLayout = this.createGraphRelationshipFormLayout();
        var grAddButton = isc.ToolStripButton.create({
            ID: "grAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
            click: function () {
                var windowGraphRelForm = window.graphRelForm;
                var graphRelForm;
                graphRelForm = windowGraphRelForm;
                graphRelForm.hideItem("common_edge_direction");
                graphRelForm.clearValues();
                graphRelForm.clearErrors(false);
                graphRelForm.editNewRecord();
                graphRelForm.getItem("common_type").enable();
                var windowGraphEdgeVertexForm = window.graphEdgeVertexForm;
                var graphEdgeVertexForm;
                graphEdgeVertexForm = windowGraphEdgeVertexForm;
                graphEdgeVertexForm.clearValues();
                graphEdgeVertexForm.getItem("_suggest").enable();
                grfWindow.setTitle("Edit Relationship Form Window");
                grfWindow.show();
            }
        });
        var grEditButton = isc.ToolStripButton.create({
            ID: "grEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
            click: function () {
                var windowGraphGridRel = window.graphGridRel;
                var graphGridRel;
                graphGridRel = windowGraphGridRel;
                if (graphGridRel != null) {
                    var lgRecord = graphGridRel.getSelectedRecord();
                    if (lgRecord != null) {
                        var windowGraphRelForm = window.graphRelForm;
                        var graphRelForm = void 0;
                        graphRelForm = windowGraphRelForm;
                        graphRelForm.hideItem("common_edge_direction");
                        graphRelForm.clearValues();
                        graphRelForm.clearErrors(false);
                        graphRelForm.editSelectedData(graphGridRel);
                        graphRelForm.getItem("common_type").disable();
                        grfWindow.setTitle("Edit Relationship Form Window");
                        var windowGraphEdgeVertexForm = window.graphEdgeVertexForm;
                        var graphEdgeVertexForm = void 0;
                        graphEdgeVertexForm = windowGraphEdgeVertexForm;
                        graphEdgeVertexForm.setValue("_suggest", graphRelForm.getValue("common_vertex_name"));
                        graphEdgeVertexForm.getItem("_suggest").disable();
                        grfWindow.show();
                    }
                    else
                        isc.say("You must select a row on the grid to edit.");
                }
            }
        });
        var grDeleteButton = isc.ToolStripButton.create({
            ID: "grDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
            click: function () {
                var windowGraphGridRel = window.graphGridRel;
                var graphGridRel;
                graphGridRel = windowGraphGridRel;
                var lgRecord = graphGridRel.getSelectedRecord();
                if (lgRecord != null) {
                    isc.confirm("Proceed with row deletion operation?", "RedisGraph.prototype.deleteSelectedGraphGridRelRow(value ? 'OK' : 'Cancel')");
                }
                else
                    isc.say("You must select a row on the grid to remove.");
            }
        });
        var grfSaveButton = isc.Button.create({
            ID: "grfSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                var windowGraphRelForm = window.graphRelForm;
                var graphRelForm;
                graphRelForm = windowGraphRelForm;
                if (graphRelForm.valuesAreValid(false, false)) {
                    var windowAppViewForm = window.appViewForm;
                    var appViewForm_1;
                    appViewForm_1 = windowAppViewForm;
                    graphRelForm.setValue("common_vertex_src_id", appViewForm_1.getValue("common_id"));
                    var windowGraphEdgeVertexForm = window.graphEdgeVertexForm;
                    var graphEdgeVertexForm = void 0;
                    graphEdgeVertexForm = windowGraphEdgeVertexForm;
                    graphRelForm.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                    windowAppContext.assignFormContext(graphRelForm);
                    graphRelForm.saveData("RedisGraph.prototype.updateCallback(dsResponse,data,dsRequest)");
                    grfWindow.hide();
                }
                else
                    graphRelForm.validate(false);
            }
        });
        var grfCancelButton = isc.IButton.create({
            ID: "grfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                grfWindow.hide();
            }
        });
        var grfButtonLayout = isc.HStack.create({
            ID: "grfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 30, membersMargin: 40,
            members: [grfSaveButton, grfCancelButton]
        });
        var graphRelFormLayout = isc.VStack.create({
            ID: "graphRelFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [graphEdgeFormLayout, grfButtonLayout]
        });
        var grfWindow = isc.Window.create({
            ID: "grfWindow", title: "Graph Relationship Form Window", autoSize: true, canDragResize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [graphRelFormLayout]
        });
        var graphToolStrip = isc.ToolStrip.create({
            ID: "graphToolStrip", width: "100%", height: 32, autoDraw: false,
            members: [grAddButton, "separator", grEditButton, "separator", grDeleteButton]
        });
        var graphGridRelOut = isc.ListGrid.create({
            ID: "graphGridRel", dataSource: windowAppContext.getAppViewRelOutDS(),
            autoDraw: false, width: 588, height: 473, autoFetchData: false, showFilterEditor: false,
            allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false,
            canEditTitles: false, canEdit: false, leaveScrollbarGap: false
        });
        var tsGridLayout = isc.VStack.create({
            ID: "tsGridLayout", width: "100%", autoDraw: false, layoutTopMargin: 10,
            members: [graphToolStrip, graphGridRelOut]
        });
        var tsGraphTab = isc.TabSet.create({
            ID: "tsGraphTab", tabBarPosition: "top", width: 600, height: 575, autoDraw: false,
            tabs: [
                { title: "Node", pane: appViewFormLayout },
                { title: "Relationships", pane: tsGridLayout,
                    tabSelected: function (tabSet, tabNum, tabPane, ID, tab) {
                        if (tabNum == 1) {
                            var primaryKeyField = scDataSource.getPrimaryKeyField();
                            var keyName = primaryKeyField.name;
                            var simpleCriteria = {};
                            simpleCriteria[keyName] = appViewForm.getValue(keyName);
                            graphGridRelOut.filterData(simpleCriteria);
                        }
                    } }
            ]
        });
        avfWindow = isc.Window.create({
            ID: "avfWindow", title: "Data Record Window", width: 609, height: 625,
            canDragResize: true, autoCenter: true, isModal: true, showModalMask: true,
            autoDraw: false,
            items: [tsGraphTab]
        });
        var tsAddButton = isc.ToolStripButton.create({
            ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
            click: function () {
                var windowAppContext = window._appContext_;
                var windowAppViewGrid = window.appViewGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var lgRecord = appViewGrid.getSelectedRecord();
                if (windowAppContext.isGraphEnabled()) {
                    avfWindow.setTitle("Add New Record Window");
                    appViewForm.clearValues();
                    appViewForm.editNewRecord();
                    var windowTSGraphTab = window.tsGraphTab;
                    var tsGraphTab_1;
                    tsGraphTab_1 = windowTSGraphTab;
                    tsGraphTab_1.disableTab(1);
                    RedisGraph.prototype.graphNodeFormsClearValues();
                }
                else {
                    if (lgRecord == null) {
                        avfWindow.setTitle("Add New Record Window");
                        appViewForm.clearValues();
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
                        avfWindow.setTitle("Add (Duplicate) Record Window");
                    }
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
                        avfWindow.setTitle("Edit Data Form Window");
                        if (windowAppContext.isGraphEnabled()) {
                            var windowTSGraphTab = window.tsGraphTab;
                            var tsGraphTab_2;
                            tsGraphTab_2 = windowTSGraphTab;
                            RedisGraph.prototype.appViewToGraphNodeForms();
                            tsGraphTab_2.enableTab(1);
                            tsGraphTab_2.selectTab(0);
                            var windowGraphRelForm = window.graphRelForm;
                            var graphRelForm = void 0;
                            RedisGraph.prototype.showGraphNodeForm(lgRecord.common_vertex_label);
                        }
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
                        isc.confirm("Proceed with row deletion operation?", "RedisGraph.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                    }
                    else
                        isc.say("You must select a row on the grid to remove.");
                }
            }
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
        var gvHTMLPane = isc.HTMLPane.create({
            width: 800, height: 600, showEdges: false, autoDraw: false,
            contentsURL: windowAppContext.getGraphVisualizationURL(),
            contentsType: "page"
        });
        var gvOptionsForm = isc.DynamicForm.create({
            ID: "gvOptionsForm", autoDraw: false, width: 500, colWidths: [190, "*"],
            fields: [
                { name: "is_matched", title: "Is Matched", type: "text", defaultValue: "false", hidden: true },
                { name: "is_hierarchical", title: "Is Hierarchical", type: "text", defaultValue: "false", hidden: true },
                { name: "node_shape", title: "Node Shape", type: "text", editorType: "ComboBoxItem", defaultValue: "ellipse", canEdit: true, required: true, hint: "Available node shapes", wrapHintText: false, valueMap: { "ellipse": "Ellipse", "circle": "Circle", "box": "Box", "square": "Square", "database": "Database", "text": "Text", "diamond": "Diamond", "star": "Star", "triangle": "Triangle", "triangleDown": "Triangle Down", "hexagon": "Hexagon" } },
                { name: "node_color", title: "Node Color", type: "color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false },
                { name: "edge_arrow", title: "Edge Arrow", type: "text", editorType: "ComboBoxItem", defaultValue: "to", canEdit: true, required: true, hint: "Available edge arrow types", wrapHintText: false, valueMap: { "to": "To", "from": "From", "to;from": "To and From", "middle": "Middle" } },
                { name: "edge_color", title: "Edge Color", type: "color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false },
                { name: "match_color", title: "Match Color", type: "color", defaultValue: "#FAA0A0", canEdit: true, required: true, hint: "Available match colors", wrapHintText: false }
            ]
        });
        var gvofApplyButton = isc.IButton.create({
            ID: "gvofApplyButton", title: "Apply", autoFit: true, autoDraw: false,
            click: function () {
                gvofWindow.hide();
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var gvofDefaultsButton = isc.IButton.create({
            ID: "gvofDefaultsButton", title: "Defaults", autoFit: true, autoDraw: false,
            click: function () {
                gvOptionsForm.clearValue("node_shape");
                gvOptionsForm.clearValue("node_color");
                gvOptionsForm.clearValue("edge_arrow");
                gvOptionsForm.clearValue("edge_color");
                gvOptionsForm.clearValue("match_color");
            }
        });
        var gvofCancelButton = isc.IButton.create({
            ID: "gvofCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                gvofWindow.hide();
            }
        });
        var gvofButtonLayout = isc.HStack.create({
            ID: "gvofButtonLayout", width: "100%", height: 24,
            layoutAlign: "center", autoDraw: false, membersMargin: 40,
            members: [gvofApplyButton, gvofDefaultsButton, gvofCancelButton]
        });
        var gvoFormLayout = isc.VStack.create({
            ID: "gvoFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [gvOptionsForm, gvofButtonLayout]
        });
        var gvofWindow = isc.Window.create({
            ID: "gvofWindow", title: "Graph Options Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [gvoFormLayout]
        });
        var gvOptionsButton = isc.IButton.create({
            ID: "gvOptionsButton", title: "Options", autoFit: true, autoDraw: false,
            click: function () {
                gvofWindow.show();
            }
        });
        var gvRefreshButton = isc.IButton.create({
            ID: "gvRefreshButton", title: "Refresh", autoFit: true, autoDraw: false,
            click: function () {
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var gvDownloadButton = isc.IButton.create({
            ID: "gvDownloadButton", title: "Download", autoFit: true, autoDraw: false,
            click: function () {
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(true));
            }
        });
        var gvCloseButton = isc.IButton.create({
            ID: "gvCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                gvWindow.hide();
            }
        });
        var gvButtonLayout = isc.HStack.create({
            ID: "gvButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40, redrawOnResize: true,
            members: [gvOptionsButton, gvRefreshButton, gvDownloadButton, gvCloseButton]
        });
        var gvFormLayout = isc.VStack.create({
            ID: "gvFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [gvHTMLPane, gvButtonLayout]
        });
        var gvWindow = isc.Window.create({
            ID: "gvWindow", title: "Graph Visualization Window", autoSize: true, canDragResize: true,
            autoCenter: true, isModal: false, showModalMask: false, autoDraw: false, redrawOnResize: true,
            items: [gvFormLayout]
        });
        var tsGraphButton = isc.ToolStripButton.create({
            ID: "tsGraphButton", icon: "[SKIN]/actions/ai-graph-icon.png", prompt: "Show Graph", autoDraw: false, showDown: false,
            click: function () {
                gvOptionsForm.setValue("is_matched", "false");
                gvOptionsForm.setValue("is_hierarchical", "false");
                gvWindow.setTitle("Graph Visualization Window");
                gvWindow.show();
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsGraphMatchedButton = isc.ToolStripButton.create({
            ID: "tsGraphMatchedButton", icon: "[SKIN]/actions/ai-graph-match-icon.png", prompt: "Show Matched Graph", autoDraw: false, showDown: false,
            click: function () {
                gvOptionsForm.setValue("is_matched", "true");
                gvOptionsForm.setValue("is_hierarchical", "false");
                gvWindow.setTitle("Graph Visualization Window (Matched)");
                gvWindow.show();
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsGraphTreeButton = isc.ToolStripButton.create({
            ID: "tsGraphTreeButton", icon: "[SKIN]/actions/ai-graph-tree-icon.png", prompt: "Show Graph As Tree", autoDraw: false, showDown: false,
            click: function () {
                gvOptionsForm.setValue("is_hierarchical", "true");
                gvWindow.setTitle("Graph Visualization Window (Tree)");
                gvWindow.show();
                gvHTMLPane.setContentsURL(RedisGraph.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsApplicationGridButton = isc.ToolStripButton.create({
            ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var windowCommandGrid = window.commandGrid;
                var windowAppLayout = window.appLayout;
                var commandGrid;
                commandGrid = windowCommandGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.showMember(appViewGrid);
                appLayout.hideMember(commandGrid);
            }
        });
        var tsCommandGridButton = isc.ToolStripButton.create({
            ID: "tsCommandGridButton", icon: "[SKIN]/actions/ai-command-list-icon.png", prompt: "Command Grid", autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var windowCommandGrid = window.commandGrid;
                var windowAppLayout = window.appLayout;
                var commandGrid;
                commandGrid = windowCommandGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.hideMember(appViewGrid);
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
                { name: "csv_header", title: "CSV Header", type: "radioGroup", defaultValue: "Title", valueMap: ["Title", "Field/Type/Title"], vertical: false }
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
                    headerHTMLFlow.setContents(RedisGraph.prototype.createHTMLHeader());
                    headerHTMLFlow.redraw();
                    var windowAppViewGrid = window.appViewGrid;
                    var appViewGrid = void 0;
                    appViewGrid = windowAppViewGrid;
                    if (setGridForm.getValue("column_filtering") == "Enabled")
                        appViewGrid.setShowFilterEditor(true);
                    else
                        appViewGrid.setShowFilterEditor(false);
                    if (setGridForm.getValue("csv_header") == "Title")
                        windowAppContext_3.setGridCSVHeader("title");
                    else
                        windowAppContext_3.setGridCSVHeader("field");
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
        var commandToolStrip;
        commandToolStrip = isc.ToolStrip.create({
            ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
            members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, "separator", tsGraphButton, tsGraphMatchedButton, tsGraphTreeButton, "starSpacer", tsApplicationGridButton, tsCommandGridButton, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
        });
        return commandToolStrip;
    };
    RedisGraph.prototype.createAppViewGrid = function () {
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
                                RedisGraph.prototype.executeAppViewGraphSearch();
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
        var appViewGrid;
        appViewGrid = CustomListGrid.create({
            ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(),
            autoDraw: false, width: "100%", height: windowAppContext.getGridHeightPercentage(),
            autoFetchData: windowAppContext.isModelerEnabled(), showFilterEditor: false,
            allowFilterOperators: false, filterOnKeypress: true, useAdvancedFieldPicker: true,
            canEditTitles: true, expansionFieldImageShowSelected: false, canExpandRecords: true,
            expansionMode: "related", detailDS: windowAppContext.getAppViewRelDS(), canEdit: false, leaveScrollbarGap: false,
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
        return appViewGrid;
    };
    RedisGraph.prototype.createCommandGrid = function () {
        var windowAppContext = window._appContext_;
        var commandGrid = isc.ListGrid.create({
            ID: "commandGrid", dataSource: "RG-DocCmdGrid", autoDraw: false, width: "100%",
            height: windowAppContext.getGridHeightPercentage(), autoFetchData: false,
            showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: false,
            useAdvancedFieldPicker: false, canEditTitles: false, expansionFieldImageShowSelected: false,
            canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, wrapCells: true, cellHeight: 50,
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
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
    RedisGraph.prototype.init = function () {
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", { multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200 });
        var windowAppContext = window._appContext_;
        var headerSection = this.createHeaderSection();
        var searchSection = this.createSearchSection();
        var commandToolStrip = this.createCommandToolStrip();
        var appViewGrid = this.createAppViewGrid();
        var commandGrid = this.createCommandGrid();
        this.appLayout = isc.VStack.create({
            ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
            members: [headerSection, searchSection, commandToolStrip, appViewGrid, commandGrid]
        });
        this.appLayout.hideMember(commandGrid);
    };
    RedisGraph.prototype.show = function () {
        this.appLayout.show();
    };
    RedisGraph.prototype.hide = function () {
        this.appLayout.hide();
    };
    return RedisGraph;
}());
//# sourceMappingURL=RedisGraph.js.map