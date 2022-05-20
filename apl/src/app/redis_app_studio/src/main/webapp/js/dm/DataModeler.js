var DataModeler = (function () {
    function DataModeler(anAppContext) {
        this.appContext = anAppContext;
    }
    DataModeler.prototype.createHTMLHeader = function () {
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
    DataModeler.prototype.createHeaderSection = function () {
        var htmlString = this.createHTMLHeader();
        var headerHTMLFlow = isc.HTMLFlow.create({ ID: "headerHTMLFlow", width: "100%", height: "5%", autoDraw: false, contents: htmlString });
        return headerHTMLFlow;
    };
    DataModeler.prototype.defaultCriteria = function (aDSStructure, aDSTitle, aFetchLimit) {
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
    DataModeler.prototype.executeAppViewGridExport = function (anAction, aFormat, aFetchLimit) {
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
    DataModeler.prototype.executeAppViewGridSearch = function () {
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
    DataModeler.prototype.createSearchSection = function () {
        var windowAppContext = window._appContext_;
        var searchForm = isc.DynamicForm.create({
            ID: "searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
            items: [{
                    type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon: true,
                    keyPress: function (item, form, keyName, characterValue) {
                        if (keyName == "Enter")
                            DataModeler.prototype.executeAppViewGridSearch();
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
                DataModeler.prototype.executeAppViewGridSearch();
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
        var tsExecuteSearch = isc.ToolStripButton.create({
            ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false,
            click: function () {
                DataModeler.prototype.executeAppViewGridSearch();
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
                appViewGrid.invalidateCache();
                appViewGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
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
                    editorType: "ComboBoxItem", optionDataSource: "DM-SuggestList",
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
                searchFilter.clearCriteria();
                searchForm.clearValues();
                suggestForm.clearValues();
                appViewGrid.invalidateCache();
                appViewGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
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
    DataModeler.prototype.deleteSelectedAppViewGridRow = function (aResponse) {
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
    DataModeler.prototype.deleteSelectedApplicationsGridRow = function (aResponse) {
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
    DataModeler.prototype.deleteSelectedDocumentsGridRow = function (aResponse) {
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
    DataModeler.prototype.deleteSelectedFlatAppViewGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowDataFlatGrid = window.dataFlatGrid;
            var dataFlatGrid_1;
            dataFlatGrid_1 = windowDataFlatGrid;
            if (dataFlatGrid_1 != null) {
                dataFlatGrid_1.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                setTimeout(function () { dataFlatGrid_1.invalidateCache(); dataFlatGrid_1.filterData({}); }, 2000);
            }
        }
    };
    DataModeler.prototype.deleteSelectedHierarchyAppViewGridRow = function (aResponse) {
        if (aResponse == "OK") {
            var windowDataHierarchyGrid = window.dataHierarchyGrid;
            var dataHierarchyGrid_1;
            dataHierarchyGrid_1 = windowDataHierarchyGrid;
            if (dataHierarchyGrid_1 != null) {
                dataHierarchyGrid_1.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                setTimeout(function () { dataHierarchyGrid_1.invalidateCache(); dataHierarchyGrid_1.filterData({}); }, 2000);
            }
        }
    };
    DataModeler.prototype.deleteSelectedGraphGridRelRow = function (aResponse) {
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
    DataModeler.prototype.updateCallback = function (aDSResponse, aData, aDSRequest) {
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
    DataModeler.prototype.uploadCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Upload complete", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    };
    DataModeler.prototype.dafNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataFlatForm = window.dafcUploadForm;
        var dafcUploadForm;
        dafcUploadForm = windowDataFlatForm;
        var windowDataFlatXForm = window.dafxUploadForm;
        var dafxUploadForm;
        dafxUploadForm = windowDataFlatXForm;
        dafxUploadForm.setValue("document_title", dafcUploadForm.getValue("document_title"));
        dafxUploadForm.setValue("document_description", dafcUploadForm.getValue("document_description"));
        dafxUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    DataModeler.prototype.jsonNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataHierarchyForm = window.jsonfUploadForm;
        var jsonfUploadForm;
        jsonfUploadForm = windowDataHierarchyForm;
        var windowDataHierarchyXForm = window.jsonxUploadForm;
        var jsonxUploadForm;
        jsonxUploadForm = windowDataHierarchyXForm;
        jsonxUploadForm.setValue("document_title", jsonfUploadForm.getValue("document_title"));
        jsonxUploadForm.setValue("document_description", jsonfUploadForm.getValue("document_description"));
        jsonxUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    DataModeler.prototype.graphEdgeUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataHierarchyForm = window.graphdUploadForm;
        var graphdUploadForm;
        graphdUploadForm = windowDataHierarchyForm;
        var windowDataHierarchyEdgeForm = window.grapheUploadForm;
        var grapheUploadForm;
        grapheUploadForm = windowDataHierarchyEdgeForm;
        grapheUploadForm.setValue("document_title", graphdUploadForm.getValue("document_title"));
        grapheUploadForm.setValue("document_description", graphdUploadForm.getValue("document_description"));
        grapheUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    DataModeler.prototype.graphNodeUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataHierarchyDataForm = window.graphdUploadForm;
        var graphdUploadForm;
        graphdUploadForm = windowDataHierarchyDataForm;
        var windowDataHierarchyNodeForm = window.graphnUploadForm;
        var graphnUploadForm;
        graphnUploadForm = windowDataHierarchyNodeForm;
        graphnUploadForm.setValue("document_title", graphdUploadForm.getValue("document_title"));
        graphnUploadForm.setValue("document_description", graphdUploadForm.getValue("document_description"));
        graphnUploadForm.saveData("DataModeler.prototype.graphEdgeUploadCallback(dsResponse,data,dsRequest)");
    };
    DataModeler.prototype.dmGenerateCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Application ready", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
        var windowDMGenAppWindow = window.dmGenAppWindow;
        var dmGenAppWindow;
        dmGenAppWindow = windowDMGenAppWindow;
        dmGenAppWindow.hide();
        var windowGenDMAppForm = window.dmGenAppForm;
        var dmGenAppForm;
        dmGenAppForm = windowGenDMAppForm;
        setTimeout(function () { window.open(dmGenAppForm.getValue("gen_link"), "_blank"); }, 2000);
    };
    DataModeler.prototype.rsGenerateCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Application ready", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
        var windowRSGenAppWindow = window.rsGenAppWindow;
        var rsGenAppWindow;
        rsGenAppWindow = windowRSGenAppWindow;
        rsGenAppWindow.hide();
        var windowGenRSAppForm = window.rsGenAppForm;
        var rsGenAppForm;
        rsGenAppForm = windowGenRSAppForm;
        setTimeout(function () { window.open(rsGenAppForm.getValue("gen_link"), "_blank"); }, 2000);
    };
    DataModeler.prototype.fieldPrefix = function (aFieldName) {
        var fieldName = aFieldName.toLowerCase();
        var offset = fieldName.indexOf("_");
        if (offset < 2)
            return fieldName;
        else
            return fieldName.substring(0, offset);
    };
    DataModeler.prototype.uniqueFormNames = function (aDS) {
        var formName;
        var fieldPrefix;
        var fieldLookup;
        var formNames = new Map();
        formNames.set("common", "Common");
        for (var _i = 0, _a = aDS.getFieldNames(false); _i < _a.length; _i++) {
            var fieldName = _a[_i];
            fieldPrefix = this.fieldPrefix(fieldName);
            fieldLookup = formNames.get(fieldPrefix);
            if (fieldLookup == undefined) {
                formName = fieldPrefix.charAt(0).toUpperCase() + fieldPrefix.slice(1);
                formNames.set(fieldPrefix, formName);
            }
        }
        return formNames;
    };
    DataModeler.prototype.createGraphRelationshipFormLayout = function () {
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
                    name: "_suggest", title: "End Node", editorType: "ComboBoxItem", optionDataSource: "DM-SuggestList", required: true,
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
    DataModeler.prototype.createGraphNodeFormLayout = function (aDS) {
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
                            uiField["changed"] = "DataModeler.prototype.showGraphNodeForm(value);";
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
                if (!key.startsWith("common")) {
                    selectNodeLabels.add(value);
                }
            });
            dynamicForm.setValueMap("common_vertex_label", selectNodeLabels);
            graphNodeFormLayout.addMember(dynamicForm);
            windowAppContext.add(formName, dynamicForm);
        });
        return graphNodeFormLayout;
    };
    DataModeler.prototype.graphNodeFormsClearValues = function () {
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
    DataModeler.prototype.graphNodeFormsIsValid = function (aShowValid) {
        var windowAppContext = window._appContext_;
        var dynamicForm;
        dynamicForm = windowAppContext.get("commonAppViewForm");
        if (aShowValid)
            dynamicForm.validate(false);
        else if (!dynamicForm.valuesAreValid(false, false))
            return false;
        return true;
    };
    DataModeler.prototype.appViewToGraphNodeForms = function () {
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
    DataModeler.prototype.graphNodesToAppViewForm = function () {
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
    DataModeler.prototype.showGraphNodeForm = function (aLabelName) {
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
    DataModeler.prototype.graphVisualizationOptionsToURL = function (isDownload) {
        var windowAppContext = window._appContext_;
        var windowGVOptionsForm = window.gvOptionsForm;
        var gvOptionsForm;
        gvOptionsForm = windowGVOptionsForm;
        var gvURL = windowAppContext.getGraphVisualizationURL() + "?";
        gvURL += "is_modeler=true&";
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
    DataModeler.prototype.createCommandToolStrip = function () {
        var windowAppContext = window._appContext_;
        var dmGenAppForm = isc.DynamicForm.create({
            ID: "dmGenAppForm", autoDraw: false, width: 500, colWidths: [190, "*"], dataSource: "DM-GenAppForm",
            fields: [
                { name: "app_group", title: "App Group", type: "text", value: "Redis App Studio", required: true, hint: "Application group", wrapHintText: false },
                { name: "app_name", title: "App Name", type: "text", defaultValue: "Data Modeler", required: true, hint: "Name of application", wrapHintText: false },
                { name: "app_prefix", title: "App Prefix", type: "text", canEdit: true, hint: "Application prefix (3 characters)", wrapHintText: false },
                { name: "app_type", title: "App Type", type: "text", value: "Data Modeler", editorType: "ComboBoxItem", canEdit: true, hint: "Application type", wrapHintText: false },
                { name: "ds_structure", title: "DS Structure", type: "text", canEdit: true, editorType: "SelectItem", hint: "Data source type selected", wrapHintText: false, wrapTitle: false },
                { name: "ds_title", title: "DS Title", type: "text", canEdit: false, hint: "Data source title selected", wrapHintText: false },
                { name: "grid_height", title: "Grid Height", editorType: "SpinnerItem", writeStackedIcons: false, hint: "Percentage of page for grid", wrapHintText: false, wrapTitle: false, defaultValue: windowAppContext.getGridHeightNumber(), min: 30, max: 100, step: 5 },
                { name: "skin_name", title: "UI Theme", type: "text", value: "Tahoe", editorType: "ComboBoxItem", canEdit: true, hint: "UI styling theme", wrapHintText: false }
            ]
        });
        var dmGenAppCreateButton = isc.Button.create({
            ID: "dmGenAppCreateButton", title: "Create", autoFit: true, autoDraw: false,
            click: function () {
                if (dmGenAppForm.valuesAreValid(false, false)) {
                    var appPrefix = dmGenAppForm.getValue("app_prefix");
                    if ((!appPrefix) || (appPrefix.length != 3))
                        isc.warn("Application prefix must be 3 characters in length.");
                    else {
                        var appPrefixUC = appPrefix.toUpperCase();
                        dmGenAppForm.setValue("app_prefix", appPrefixUC);
                        dmGenAppForm.saveData("DataModeler.prototype.dmGenerateCallback(dsResponse,data,dsRequest)");
                    }
                }
                else
                    dmGenAppForm.validate(false);
            }
        });
        var dmGenAppCancelButton = isc.IButton.create({
            ID: "dmGenAppCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                dmGenAppWindow.hide();
            }
        });
        var dmGenAppButtonLayout = isc.HStack.create({
            ID: "dmGenAppButtonLayout", width: "100%", height: 24,
            layoutAlign: "center", autoDraw: false, membersMargin: 40,
            members: [dmGenAppCreateButton, dmGenAppCancelButton]
        });
        var dmGenAppFormLayout = isc.VStack.create({
            ID: "dmGenAppFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [dmGenAppForm, dmGenAppButtonLayout]
        });
        var dmGenAppWindow = isc.Window.create({
            ID: "dmGenAppWindow", title: "Generate Application Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [dmGenAppFormLayout]
        });
        var dafcUploadForm = isc.DynamicForm.create({
            ID: "dafcUploadForm", width: 275, height: 50, autoDraw: false,
            dataSource: "DM-DataFlatGrid",
            fields: [
                { name: "document_title", title: "Title", type: "text", required: true },
                { name: "document_description", title: "Description", type: "text", defaultValue: "None", required: true },
                { name: "document_file", title: "Data File", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var dafxUploadForm = isc.DynamicForm.create({
            ID: "dafxUploadForm", width: 275, height: 20, autoDraw: false,
            dataSource: "DM-DataFlatGrid",
            fields: [
                { name: "document_file", title: "Schema File", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var dafcxFormLayout = isc.VStack.create({
            ID: "dafcxFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
            members: [dafcUploadForm, dafxUploadForm]
        });
        var dafSaveButton = isc.Button.create({
            ID: "dafSaveButton", title: "Upload", autoFit: true, autoDraw: false,
            click: function () {
                if ((dafcUploadForm.valuesAreValid(false, false)) &&
                    (dafxUploadForm.valuesAreValid(false, false))) {
                    var csvFileName = void 0;
                    csvFileName = dafcUploadForm.getValue("document_file");
                    var xmlFileName = void 0;
                    xmlFileName = dafxUploadForm.getValue("document_file");
                    if (!csvFileName.toLowerCase().endsWith(".csv"))
                        isc.say("Data file name must end with a 'csv' extension");
                    else if (!xmlFileName.toLowerCase().endsWith(".xml"))
                        isc.say("Schema file name must end with a 'xml' extension");
                    else {
                        dafcUploadForm.saveData("DataModeler.prototype.dafNextUploadCallback(dsResponse,data,dsRequest)");
                        dafWindow.hide();
                    }
                }
                else {
                    dafcUploadForm.validate(false);
                    dafxUploadForm.validate(false);
                }
            }
        });
        var dafCancelButton = isc.IButton.create({
            ID: "dafCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                dafWindow.hide();
            }
        });
        var dafButtonLayout = isc.HStack.create({
            ID: "dafButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [dafSaveButton, dafCancelButton]
        });
        var dafUploadFormLayout = isc.VStack.create({
            ID: "dafUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [dafcxFormLayout, dafButtonLayout]
        });
        var dafWindow = isc.Window.create({
            ID: "dafWindow", title: "Flat Data Upload Form Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [dafUploadFormLayout]
        });
        var applicationsGrid = isc.ListGrid.create({
            ID: "applicationsGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-ApplicationGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
            cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
            recordDoubleClick: function () {
                var lgRecord = applicationsGrid.getSelectedRecord();
                if (lgRecord != null) {
                    setTimeout(function () { window.open(lgRecord.document_link, "_blank"); }, 100);
                }
            },
            fields: [
                { name: "document_name", title: "Name", width: 200 },
                { name: "document_title", title: "Title", width: 200 },
                { name: "document_type", title: "Type", width: 75 },
                { name: "document_description", title: "Description", width: 200 },
                { name: "document_date", title: "Upload Date", width: 100 },
                { name: "document_owner", title: "Owner", width: 75 }
            ]
        });
        var ragDeleteButton = isc.Button.create({
            ID: "ragDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = applicationsGrid.getSelectedRecord();
                if (lgRecord != null) {
                    if (lgRecord.document_owner != "System")
                        isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedApplicationsGridRow(value ? 'OK' : 'Cancel')");
                    else
                        isc.say("You cannot delete a 'System' owned application.");
                }
                else
                    isc.say("You must select a row on the grid to remove.");
            }
        });
        var ragCloseButton = isc.IButton.create({
            ID: "ragCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                ragWindow.hide();
            }
        });
        var ragButtonLayout = isc.HStack.create({
            ID: "ragButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [ragDeleteButton, ragCloseButton]
        });
        var ragFormLayout = isc.VStack.create({
            ID: "ragFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [applicationsGrid, ragButtonLayout]
        });
        var ragWindow = isc.Window.create({
            ID: "ragWindow", title: "Applications Manager Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [ragFormLayout]
        });
        var dataFlatGrid = isc.ListGrid.create({
            ID: "dataFlatGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DataFlatGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
            cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
            fields: [
                { name: "document_name", title: "Name", width: 200 },
                { name: "document_title", title: "Title", width: 200 },
                { name: "document_type", title: "Type", width: 75 },
                { name: "document_description", title: "Description", width: 200 },
                { name: "document_date", title: "Upload Date", width: 100 },
                { name: "document_size", title: "File Size", width: 75 }
            ]
        });
        var dafgGenerateButton = isc.Button.create({
            ID: "dafgGenerateButton", title: "Generate ...", autoFit: true, autoDraw: false, disabled: false,
            click: function () {
                var lgRecord = dataFlatGrid.getSelectedRecord();
                if (lgRecord != null) {
                    dmGenAppForm.clearErrors(false);
                    dmGenAppForm.clearValues();
                    dmGenAppForm.setValue("ds_structure", "Flat");
                    dmGenAppForm.setValue("ds_title", lgRecord.document_title);
                    dmGenAppForm.setValue("grid_height", "80");
                    dmGenAppWindow.show();
                }
                else
                    isc.say("You must select a row to generate application.");
            }
        });
        var dafgDeleteButton = isc.Button.create({
            ID: "dafgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = dataFlatGrid.getSelectedRecord();
                if (lgRecord != null) {
                    isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedFlatAppViewGridRow(value ? 'OK' : 'Cancel')");
                }
                else
                    isc.say("You must select a row on the grid to remove.");
            }
        });
        var dafgCloseButton = isc.IButton.create({
            ID: "dafgCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                dafgWindow.hide();
            }
        });
        var dafgButtonLayout = isc.HStack.create({
            ID: "dafgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [dafgGenerateButton, dafgDeleteButton, dafgCloseButton]
        });
        var dafgFormLayout = isc.VStack.create({
            ID: "dafgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [dataFlatGrid, dafgButtonLayout]
        });
        var dafgWindow = isc.Window.create({
            ID: "dafgWindow", title: "Flat Data Manager Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [dafgFormLayout]
        });
        var jsonfUploadForm = isc.DynamicForm.create({
            ID: "jsonfUploadForm", width: 275, height: 75, autoDraw: false,
            dataSource: "DM-DataHierJSON",
            fields: [
                { name: "document_title", title: "Title", type: "text", required: true },
                { name: "document_description", title: "Description", type: "text", defaultValue: "None", required: true },
                { name: "document_file", title: "JSON File", type: "binary", required: true }
            ]
        });
        var jsonxUploadForm = isc.DynamicForm.create({
            ID: "jsonxUploadForm", width: 275, height: 20, autoDraw: false,
            dataSource: "DM-DataHierJSON",
            fields: [
                { name: "document_file", title: "Schema File", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var jsonfxFormLayout = isc.VStack.create({
            ID: "jsonfxFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
            members: [jsonfUploadForm, jsonxUploadForm]
        });
        var jsonSaveButton = isc.Button.create({
            ID: "jsonSaveButton", title: "Upload", autoFit: true, autoDraw: false,
            click: function () {
                if ((jsonfUploadForm.valuesAreValid(false, false)) &&
                    (jsonxUploadForm.valuesAreValid(false, false))) {
                    var jsonDataFileName = void 0;
                    jsonDataFileName = jsonfUploadForm.getValue("document_file");
                    var xmlSchemaFileName = void 0;
                    xmlSchemaFileName = jsonxUploadForm.getValue("document_file");
                    if (!jsonDataFileName.toLowerCase().endsWith(".json"))
                        isc.say("JSON data file name must end with a 'json' extension");
                    else if (!xmlSchemaFileName.toLowerCase().endsWith(".xml"))
                        isc.say("Schema file name must end with a 'xml' extension");
                    else {
                        jsonfUploadForm.saveData("DataModeler.prototype.jsonNextUploadCallback(dsResponse,data,dsRequest)");
                        jsonWindow.hide();
                    }
                }
                else {
                    jsonfUploadForm.validate(false);
                    jsonxUploadForm.validate(false);
                }
            }
        });
        var jsonCancelButton = isc.IButton.create({
            ID: "jsonCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                jsonWindow.hide();
            }
        });
        var jsonButtonLayout = isc.HStack.create({
            ID: "jsonButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [jsonSaveButton, jsonCancelButton]
        });
        var jsonUploadFormLayout = isc.VStack.create({
            ID: "jsonUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [jsonfxFormLayout, jsonButtonLayout]
        });
        var jsonWindow = isc.Window.create({
            ID: "jsonWindow", title: "JSON Upload Form Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [jsonUploadFormLayout]
        });
        var graphdUploadForm = isc.DynamicForm.create({
            ID: "graphdUploadForm", width: 275, height: 50, autoDraw: false,
            dataSource: "DM-DataHierGraph",
            fields: [
                { name: "document_title", title: "Title", type: "text", required: true },
                { name: "document_description", title: "Description", type: "text", defaultValue: "None", required: true },
                { name: "document_file", title: "Graph Data", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var graphnUploadForm = isc.DynamicForm.create({
            ID: "graphnUploadForm", width: 275, height: 20, autoDraw: false,
            dataSource: "DM-DataHierGraph",
            fields: [
                { name: "document_file", title: "Graph Nodes", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var grapheUploadForm = isc.DynamicForm.create({
            ID: "grapheUploadForm", width: 275, height: 20, autoDraw: false,
            dataSource: "DM-DataHierGraph",
            fields: [
                { name: "document_file", title: "Graph Edges", type: "binary", wrapTitle: false, required: true }
            ]
        });
        var graphdneFormLayout = isc.VStack.create({
            ID: "graphdneFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
            members: [graphdUploadForm, graphnUploadForm, grapheUploadForm]
        });
        var graphSaveButton = isc.Button.create({
            ID: "graphSaveButton", title: "Upload", autoFit: true, autoDraw: false,
            click: function () {
                if ((graphdUploadForm.valuesAreValid(false, false)) &&
                    (graphnUploadForm.valuesAreValid(false, false)) &&
                    (grapheUploadForm.valuesAreValid(false, false))) {
                    var csvDataFileName = void 0;
                    csvDataFileName = graphdUploadForm.getValue("document_file");
                    var xmlNodesFileName = void 0;
                    xmlNodesFileName = graphnUploadForm.getValue("document_file");
                    var xmlEdgesFileName = void 0;
                    xmlEdgesFileName = grapheUploadForm.getValue("document_file");
                    if (!csvDataFileName.toLowerCase().endsWith(".csv"))
                        isc.say("Graph data file name must end with a 'csv' extension");
                    else if (!xmlNodesFileName.toLowerCase().endsWith(".xml"))
                        isc.say("Graph nodes file name must end with a 'xml' extension");
                    else if (!xmlEdgesFileName.toLowerCase().endsWith(".xml"))
                        isc.say("Graph edges file name must end with a 'xml' extension");
                    else {
                        graphdUploadForm.saveData("DataModeler.prototype.graphNodeUploadCallback(dsResponse,data,dsRequest)");
                        graphWindow.hide();
                    }
                }
                else {
                    graphdUploadForm.validate(false);
                    graphnUploadForm.validate(false);
                    grapheUploadForm.validate(false);
                }
            }
        });
        var graphCancelButton = isc.IButton.create({
            ID: "graphCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                graphWindow.hide();
            }
        });
        var graphButtonLayout = isc.HStack.create({
            ID: "graphButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [graphSaveButton, graphCancelButton]
        });
        var graphUploadFormLayout = isc.VStack.create({
            ID: "graphUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [graphdneFormLayout, graphButtonLayout]
        });
        var graphWindow = isc.Window.create({
            ID: "graphWindow", title: "Graph Data Upload Form Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [graphUploadFormLayout]
        });
        var dataHierarchyGrid = isc.ListGrid.create({
            ID: "dataHierarchyGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DataHierGraph",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            showHeaderContextMenu: false, autoSaveEdits: false, canEdit: false, wrapCells: true,
            cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
            fields: [
                { name: "document_name", title: "Name", width: 200 },
                { name: "document_title", title: "Title", width: 200 },
                { name: "document_type", title: "Type", width: 100 },
                { name: "document_description", title: "Description", width: 200 },
                { name: "document_date", title: "Upload Date", width: 100 },
                { name: "document_size", title: "File Size", width: 75 }
            ]
        });
        var dahgGenerateButton = isc.Button.create({
            ID: "dahgGenerateButton", title: "Generate", autoFit: true, autoDraw: false, disabled: false,
            click: function () {
                var lgRecord = dataHierarchyGrid.getSelectedRecord();
                if (lgRecord != null) {
                    dmGenAppForm.clearErrors(false);
                    dmGenAppForm.clearValues();
                    dmGenAppForm.setValue("ds_structure", "Hierarchy");
                    dmGenAppForm.setValue("ds_title", lgRecord.document_title);
                    dmGenAppForm.setValue("grid_height", "80");
                    dmGenAppWindow.show();
                }
                else
                    isc.say("You must select a row to generate application.");
            }
        });
        var dahgDeleteButton = isc.Button.create({
            ID: "dahgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
            click: function () {
                var lgRecord = dataHierarchyGrid.getSelectedRecord();
                if (lgRecord != null) {
                    isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedHierarchyAppViewGridRow(value ? 'OK' : 'Cancel')");
                }
                else
                    isc.say("You must select a row on the grid to remove.");
            }
        });
        var dahgCloseButton = isc.IButton.create({
            ID: "dahgCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                dahgWindow.hide();
            }
        });
        var dahgButtonLayout = isc.HStack.create({
            ID: "dahgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [dahgGenerateButton, dahgDeleteButton, dahgCloseButton]
        });
        var dahgFormLayout = isc.VStack.create({
            ID: "dahgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [dataHierarchyGrid, dahgButtonLayout]
        });
        var dahgWindow = isc.Window.create({
            ID: "dahgWindow", title: "Hierarchy Data Manager Window", autoSize: true,
            autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
            items: [dahgFormLayout]
        });
        var docUploadForm = isc.DynamicForm.create({
            ID: "docUploadForm", width: 275, height: 75, autoDraw: false,
            dataSource: "DM-DocumentGrid",
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
                    docUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
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
            ID: "documentsGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DocumentGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
            cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
            fields: [
                { name: "document_name", title: "Name", width: 200 },
                { name: "document_title", title: "Title", width: 200 },
                { name: "document_type", title: "Type", width: 75 },
                { name: "document_description", title: "Description", width: 200 },
                { name: "document_date", title: "Upload Date", width: 100 },
                { name: "document_size", title: "File Size", width: 75 }
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
                    isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedDocumentsGridRow(value ? 'OK' : 'Cancel')");
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
        var rsGenAppForm = isc.DynamicForm.create({
            ID: "rsGenAppForm", autoDraw: false, width: 500, colWidths: [190, "*"], dataSource: "DM-GenAppForm",
            fields: [
                { name: "app_group", title: "App Group", type: "text", value: "Redis App Studio", required: true, hint: "Application group", wrapHintText: false },
                { name: "app_name", title: "App Name", type: "text", required: true, hint: "Name of application", wrapHintText: false },
                { name: "app_prefix", title: "App Prefix", type: "text", canEdit: true, hint: "Application prefix (3 characters)", wrapHintText: false },
                { name: "app_type", title: "App Type", type: "text", editorType: "ComboBoxItem", canEdit: true, hint: "Application type", wrapHintText: false },
                { name: "ds_structure", title: "DS Structure", type: "text", canEdit: true, editorType: "SelectItem", hint: "Data source type selected", wrapHintText: false, wrapTitle: false },
                { name: "ds_title", title: "DS Title", type: "text", canEdit: false, hint: "Data source title selected", wrapHintText: false },
                { name: "grid_height", title: "Grid Height", editorType: "SpinnerItem", writeStackedIcons: false, hint: "Percentage of page for grid", wrapHintText: false, wrapTitle: false, defaultValue: windowAppContext.getGridHeightNumber(), min: 30, max: 100, step: 5 },
                { name: "rc_storage_type", title: "Storage Type", type: "text", editorType: "ComboBoxItem", canEdit: true, hint: "Row storage type", wrapHintText: false },
                { name: "ui_facets", title: "UI Facets", type: "radioGroup", defaultValue: "Disabled", valueMap: ["Disabled", "Enabled"], vertical: false },
                { name: "skin_name", title: "UI Theme", type: "text", value: "Tahoe", editorType: "ComboBoxItem", canEdit: true, hint: "UI styling theme", wrapHintText: false }
            ]
        });
        var rsGenAppCreateButton = isc.Button.create({
            ID: "rsGenAppCreateButton", title: "Create", autoFit: true, autoDraw: false,
            click: function () {
                if (rsGenAppForm.valuesAreValid(false, false)) {
                    var appPrefix = rsGenAppForm.getValue("app_prefix");
                    if ((!appPrefix) || (appPrefix.length != 3))
                        isc.warn("Application prefix must be 3 characters in length.");
                    else {
                        var appPrefixUC = appPrefix.toUpperCase();
                        rsGenAppForm.setValue("app_prefix", appPrefixUC);
                        rsGenAppForm.saveData("DataModeler.prototype.rsGenerateCallback(dsResponse,data,dsRequest)");
                    }
                }
                else
                    rsGenAppForm.validate(false);
            }
        });
        var rsGenAppCancelButton = isc.IButton.create({
            ID: "rsGenAppCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
            click: function () {
                rsGenAppWindow.hide();
            }
        });
        var rsGenAppButtonLayout = isc.HStack.create({
            ID: "rsGenAppButtonLayout", width: "100%", height: 24,
            layoutAlign: "center", autoDraw: false, membersMargin: 40,
            members: [rsGenAppCreateButton, rsGenAppCancelButton]
        });
        var rsGenAppFormLayout = isc.VStack.create({
            ID: "rsGenAppFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
            layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
            members: [rsGenAppForm, rsGenAppButtonLayout]
        });
        var rsGenAppWindow = isc.Window.create({
            ID: "rsGenAppWindow", title: "Generate Application Window", autoSize: true, autoCenter: true,
            isModal: true, showModalMask: true, autoDraw: false,
            items: [rsGenAppFormLayout]
        });
        var fileMenu;
        if (windowAppContext.isModelerEnabled()) {
            fileMenu = isc.Menu.create({
                ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                data: [
                    { title: "Applications ...", icon: "[SKIN]/actions/ai-application-commands-icon.png",
                        submenu: [
                            { title: "Manage", icon: "[SKIN]/actions/ai-application-manage-icon.png", enabled: true, click: function () {
                                    applicationsGrid.deselectAllRecords();
                                    ragWindow.show();
                                } }
                        ] },
                    { isSeparator: true },
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
                            { title: "Save As ...", icon: "[SKIN]/actions/ai-redis-save-as-icon.png", enabled: true, submenu: [
                                    { title: "RedisCore", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isStructureFlat(), click: function () {
                                            var windowAppContext = window._appContext_;
                                            rsGenAppForm.clearErrors(false);
                                            rsGenAppForm.clearValue("app_prefix");
                                            rsGenAppForm.setValue("app_name", "Redis Application");
                                            rsGenAppForm.setValue("app_type", "Redis Core");
                                            rsGenAppForm.setValue("ds_structure", "Flat");
                                            rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                            rsGenAppForm.setValue("grid_height", "90");
                                            rsGenAppForm.showItem("rc_storage_type");
                                            rsGenAppForm.hideItem("ui_facets");
                                            rsGenAppWindow.show();
                                        } },
                                    { title: "RedisJSON", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isJsonEnabled(), click: function () {
                                            var windowAppContext = window._appContext_;
                                            rsGenAppForm.clearErrors(false);
                                            rsGenAppForm.clearValue("app_prefix");
                                            rsGenAppForm.setValue("app_name", "Document Application");
                                            rsGenAppForm.setValue("app_type", "RedisJSON");
                                            rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                            rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                            rsGenAppForm.setValue("grid_height", "90");
                                            rsGenAppForm.hideItem("rc_storage_type");
                                            rsGenAppForm.hideItem("ui_facets");
                                            rsGenAppWindow.show();
                                        } },
                                    { title: "RediSearch", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isStructureFlat() | windowAppContext.isJsonEnabled(), click: function () {
                                            var windowAppContext = window._appContext_;
                                            rsGenAppForm.clearErrors(false);
                                            rsGenAppForm.clearValue("app_prefix");
                                            rsGenAppForm.setValue("app_name", "Search Application");
                                            rsGenAppForm.setValue("app_type", "RediSearch");
                                            if (windowAppContext.isStructureFlat())
                                                rsGenAppForm.setValue("ds_structure", "Flat");
                                            else
                                                rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                            rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                            rsGenAppForm.setValue("grid_height", "80");
                                            rsGenAppForm.hideItem("rc_storage_type");
                                            rsGenAppForm.showItem("ui_facets");
                                            rsGenAppWindow.show();
                                        } },
                                    { title: "RedisGraph", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isGraphEnabled(), click: function () {
                                            var windowAppContext = window._appContext_;
                                            rsGenAppForm.clearErrors(false);
                                            rsGenAppForm.clearValue("app_prefix");
                                            rsGenAppForm.setValue("app_name", "Graph Application");
                                            rsGenAppForm.setValue("app_type", "RedisGraph");
                                            rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                            rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                            rsGenAppForm.setValue("grid_height", "80");
                                            rsGenAppForm.hideItem("ui_facets");
                                            rsGenAppForm.hideItem("rc_storage_type");
                                            rsGenAppWindow.show();
                                        } }
                                ] }
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
                                    DataModeler.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                                } },
                            { title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function () {
                                    DataModeler.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                                } },
                            { title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function () {
                                    DataModeler.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                                } }
                        ] }
                ]
            });
        }
        else {
            fileMenu = isc.Menu.create({
                ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                data: [
                    { title: "Applications ...", icon: "[SKIN]/actions/ai-application-commands-icon.png",
                        submenu: [
                            { title: "Manage", icon: "[SKIN]/actions/ai-application-manage-icon.png", enabled: true, click: function () {
                                    applicationsGrid.deselectAllRecords();
                                    ragWindow.show();
                                } }
                        ] },
                    { title: "Flat Data ...", icon: "[SKIN]/actions/ai-data-flat-command-icon.png",
                        submenu: [
                            { title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-data-flat-upload-icon.png", click: function () {
                                    dafcUploadForm.clearValues();
                                    dafxUploadForm.clearValues();
                                    dafWindow.show();
                                } },
                            { title: "Manage", icon: "[SKIN]/actions/ai-data-flat-manage.png", enabled: true, click: function () {
                                    dataFlatGrid.deselectAllRecords();
                                    dataFlatGrid.invalidateCache();
                                    dataFlatGrid.filterData({});
                                    dafgWindow.show();
                                } }
                        ] },
                    { title: "Hierarchy Data ...", icon: "[SKIN]/actions/ai-data-hierarchy-command-icon.png",
                        submenu: [
                            { title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-icon.png",
                                submenu: [
                                    { title: "JSON", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-json-icon.png", click: function () {
                                            jsonfUploadForm.clearValues();
                                            jsonxUploadForm.clearValues();
                                            jsonWindow.show();
                                        } },
                                    { title: "Graph", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-graph-icon.png", click: function () {
                                            graphnUploadForm.clearValues();
                                            grapheUploadForm.clearValues();
                                            graphWindow.show();
                                        } }
                                ] },
                            { title: "Manage", icon: "[SKIN]/actions/ai-data-hierarchy-manage-icon.png", enabled: true, click: function () {
                                    dataHierarchyGrid.deselectAllRecords();
                                    dataHierarchyGrid.invalidateCache();
                                    dataHierarchyGrid.filterData({});
                                    dahgWindow.show();
                                } }
                        ] },
                    { isSeparator: true },
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
                        ] }
                ]
            });
        }
        var fileMenuButton = isc.ToolStripMenuButton.create({
            ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
        });
        var tsSchemaButton;
        var scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled())) {
            var schemaGrid_1;
            if (scDataSource.getFieldNames(false).length > 18) {
                schemaGrid_1 = isc.ListGrid.create({
                    ID: "schemaGrid", width: 710, height: 500, autoDraw: false, dataSource: "DM-SchemaGrid",
                    initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                    autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                    alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                    listEndEditAction: "next", autoSaveEdits: false,
                    getCellCSSText: function (record, rowNum, colNum) {
                        if (colNum == 0)
                            return "font-weight:bold; color:#000000;";
                    }
                });
            }
            else {
                schemaGrid_1 = isc.ListGrid.create({
                    ID: "schemaGrid", width: 710, height: 300, autoDraw: false, dataSource: "DM-SchemaGrid",
                    initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                    autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
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
                    schemaGrid_1.saveAllEdits();
                    isc.Notify.addMessage("Updates saved", null, null, {
                        canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                    });
                }
            });
            var sgDiscardButton = isc.Button.create({
                ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
                click: function () {
                    schemaGrid_1.discardAllEdits();
                    isc.Notify.addMessage("Updates discarded", null, null, {
                        canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                    });
                }
            });
            var sgCloseButton = isc.IButton.create({
                ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                click: function () {
                    sgWindow_1.hide();
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
                members: [schemaGrid_1, sgButtonLayout]
            });
            var sgWindow_1 = isc.Window.create({
                ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                isModal: true, showModalMask: true, autoDraw: false,
                items: [sgFormLayout]
            });
            tsSchemaButton = isc.ToolStripButton.create({
                ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                click: function () {
                    schemaGrid_1.invalidateCache();
                    schemaGrid_1.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function (aDSResponse, aData, aDSRequest) {
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
                    });
                    sgWindow_1.show();
                }
            });
        }
        else {
            var nodeSchemaGrid_1 = isc.ListGrid.create({
                ID: "nodeSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "DM-NodeSchemaGrid",
                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                listEndEditAction: "next", autoSaveEdits: false,
                getCellCSSText: function (record, rowNum, colNum) {
                    if (colNum == 0)
                        return "font-weight:bold; color:#000000;";
                }
            });
            var relSchemaGrid_1 = isc.ListGrid.create({
                ID: "relSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "DM-RelSchemaGrid",
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
                    { title: "Node Schema", pane: nodeSchemaGrid_1 },
                    { title: "Relationship Schema", pane: relSchemaGrid_1 }
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
                        nodeSchemaGrid_1.saveAllEdits();
                        isc.Notify.addMessage("Node updates saved", null, null, {
                            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                        });
                    }
                    else {
                        relSchemaGrid_1.saveAllEdits();
                        isc.Notify.addMessage("Relationship updates saved", null, null, {
                            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                        });
                    }
                }
            });
            var sgDiscardButton = isc.Button.create({
                ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
                click: function () {
                    nodeSchemaGrid_1.discardAllEdits();
                    relSchemaGrid_1.discardAllEdits();
                    isc.Notify.addMessage("Updates discarded", null, null, {
                        canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                    });
                }
            });
            var sgCloseButton = isc.IButton.create({
                ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                click: function () {
                    sgWindow_2.hide();
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
            var sgWindow_2 = isc.Window.create({
                ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                isModal: true, showModalMask: true, autoDraw: false,
                items: [sgFormLayout]
            });
            tsSchemaButton = isc.ToolStripButton.create({
                ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                click: function () {
                    nodeSchemaGrid_1.invalidateCache();
                    nodeSchemaGrid_1.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function (aDSResponse, aData, aDSRequest) {
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
                            for (var _i = 0, listGridFields_2 = listGridFields; _i < listGridFields_2.length; _i++) {
                                var lgField = listGridFields_2[_i];
                                if (gridRecord.item_name == lgField.name) {
                                    isVisible = true;
                                    gridRecord.item_title = lgField.title;
                                }
                            }
                            gridRecord.isVisible = isVisible;
                        }
                        relSchemaGrid_1.invalidateCache();
                        relSchemaGrid_1.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                    });
                    sgWindow_2.show();
                }
            });
        }
        var avfWindow;
        var appViewForm;
        var appViewFormLayout;
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled())) {
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
                        appViewForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
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
            appViewFormLayout = isc.VStack.create({
                ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                members: [appViewForm, avfButtonLayout]
            });
            avfWindow = isc.Window.create({
                ID: "avfWindow", title: "Data Record Window", autoSize: true, canDragResize: true,
                autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                items: [appViewFormLayout]
            });
        }
        else {
            appViewForm = isc.DynamicForm.create({
                ID: "appViewForm", autoDraw: false, dataSource: windowAppContext.getAppViewDS()
            });
            var avfSaveButton = isc.Button.create({
                ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                click: function () {
                    if (DataModeler.prototype.graphNodeFormsIsValid(false)) {
                        DataModeler.prototype.graphNodesToAppViewForm();
                        windowAppContext.assignFormContext(appViewForm);
                        appViewForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
                        avfWindow.hide();
                    }
                    else
                        DataModeler.prototype.graphNodeFormsIsValid(true);
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
                    grfWindow_1.setTitle("Edit Relationship Form Window");
                    grfWindow_1.show();
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
                            grfWindow_1.setTitle("Edit Relationship Form Window");
                            var windowGraphEdgeVertexForm = window.graphEdgeVertexForm;
                            var graphEdgeVertexForm = void 0;
                            graphEdgeVertexForm = windowGraphEdgeVertexForm;
                            graphEdgeVertexForm.setValue("_suggest", graphRelForm.getValue("common_vertex_name"));
                            graphEdgeVertexForm.getItem("_suggest").disable();
                            grfWindow_1.show();
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
                        isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedGraphGridRelRow(value ? 'OK' : 'Cancel')");
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
                        graphRelForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
                        grfWindow_1.hide();
                    }
                    else
                        graphRelForm.validate(false);
                }
            });
            var grfCancelButton = isc.IButton.create({
                ID: "grfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                click: function () {
                    grfWindow_1.hide();
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
            var grfWindow_1 = isc.Window.create({
                ID: "grfWindow", title: "Graph Relationship Form Window", autoSize: true, canDragResize: true,
                autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                items: [graphRelFormLayout]
            });
            var graphToolStrip = isc.ToolStrip.create({
                ID: "graphToolStrip", width: "100%", height: 32, autoDraw: false,
                members: [grAddButton, "separator", grEditButton, "separator", grDeleteButton]
            });
            var graphGridRelOut_1 = isc.ListGrid.create({
                ID: "graphGridRel", dataSource: windowAppContext.getAppViewRelOutDS(),
                autoDraw: false, width: 588, height: 473, autoFetchData: false, showFilterEditor: false,
                allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false,
                canEditTitles: false, canEdit: false, leaveScrollbarGap: false
            });
            var tsGridLayout = isc.VStack.create({
                ID: "tsGridLayout", width: "100%", autoDraw: false, layoutTopMargin: 10,
                members: [graphToolStrip, graphGridRelOut_1]
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
                                graphGridRelOut_1.filterData(simpleCriteria);
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
        }
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
                    var tsGraphTab = void 0;
                    tsGraphTab = windowTSGraphTab;
                    tsGraphTab.disableTab(1);
                    DataModeler.prototype.graphNodeFormsClearValues();
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
                            var tsGraphTab = void 0;
                            tsGraphTab = windowTSGraphTab;
                            DataModeler.prototype.appViewToGraphNodeForms();
                            tsGraphTab.enableTab(1);
                            tsGraphTab.selectTab(0);
                            var windowGraphRelForm = window.graphRelForm;
                            var graphRelForm = void 0;
                            DataModeler.prototype.showGraphNodeForm(lgRecord.common_vertex_label);
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
                        isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
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
        var analyzeGrid = isc.ListGrid.create({
            ID: "analyzeGrid", width: 700, height: 400, autoDraw: false, dataSource: "DM-AnalyzeGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, alternateRecordStyles: true,
            alternateFieldStyles: false, baseStyle: "alternateGridCell", showHeaderContextMenu: false,
            autoSaveEdits: false, canEdit: false, wrapCells: true, cellHeight: 36,
            initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
            getCellCSSText: function (record, rowNum, colNum) {
                if (colNum == 0)
                    return "font-weight:bold; color:#000000;";
            }
        });
        var agCloseButton = isc.IButton.create({
            ID: "agCloseButton", title: "Close", autoFit: true, autoDraw: false,
            click: function () {
                agWindow.hide();
            }
        });
        var agButtonLayout = isc.HStack.create({
            ID: "agButtonLayout", width: "100%", height: 24, layoutAlign: "center",
            autoDraw: false, membersMargin: 40,
            members: [agCloseButton]
        });
        var agFormLayout;
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled())) {
            agFormLayout = isc.VStack.create({
                ID: "agFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                members: [analyzeGrid, agButtonLayout]
            });
        }
        else {
            var analyzeRelGrid = isc.ListGrid.create({
                ID: "analyzeRelGrid", width: 700, height: 400, autoDraw: false, dataSource: "DM-AnalyzeRelGrid",
                autoFetchData: true, canRemoveRecords: false, canSort: false, alternateRecordStyles: true,
                alternateFieldStyles: false, baseStyle: "alternateGridCell", showHeaderContextMenu: false,
                autoSaveEdits: false, canEdit: false, wrapCells: true, cellHeight: 36,
                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                getCellCSSText: function (record, rowNum, colNum) {
                    if (colNum == 0)
                        return "font-weight:bold; color:#000000;";
                }
            });
            var agGraphAnalyzeTab = isc.TabSet.create({
                ID: "agGraphAnalyzeTab", tabBarPosition: "top", width: 720, height: 445, autoDraw: false,
                tabs: [
                    { title: "Node Grid", pane: analyzeGrid },
                    { title: "Relationship Grid", pane: analyzeRelGrid }
                ]
            });
            agFormLayout = isc.VStack.create({
                ID: "agFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                members: [agGraphAnalyzeTab, agButtonLayout]
            });
        }
        var agWindow = isc.Window.create({
            ID: "agWindow", title: "Data Analyzer Window", autoSize: true,
            autoCenter: true, isModal: false, showModalMask: false, autoDraw: false,
            items: [agFormLayout]
        });
        var tsAnalyzeButton = isc.ToolStripButton.create({
            ID: "tsAnalyzeButton", icon: "[SKIN]/actions/ai-analyze-icon.png", prompt: "Analyze Rows", autoDraw: false, showDown: false,
            click: function () {
                var windowAnalyzeGrid = window.analyzeGrid;
                var analyzeGrid;
                analyzeGrid = windowAnalyzeGrid;
                analyzeGrid.invalidateCache();
                agWindow.show();
            }
        });
        var tsChartButton = isc.ToolStripButton.create({
            ID: "tsChartButton", icon: "[SKIN]/actions/ai-chart-icon.png", prompt: "Show Chart", autoDraw: false, showDown: false,
            click: function () {
                isc.say("Data visualization is not enabled for this configuration.");
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
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
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
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var gvDownloadButton = isc.IButton.create({
            ID: "gvDownloadButton", title: "Download", autoFit: true, autoDraw: false,
            click: function () {
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(true));
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
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsGraphMatchedButton = isc.ToolStripButton.create({
            ID: "tsGraphMatchedButton", icon: "[SKIN]/actions/ai-graph-match-icon.png", prompt: "Show Matched Graph", autoDraw: false, showDown: false,
            click: function () {
                gvOptionsForm.setValue("is_matched", "true");
                gvOptionsForm.setValue("is_hierarchical", "false");
                gvWindow.setTitle("Graph Visualization Window (Matched)");
                gvWindow.show();
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsGraphTreeButton = isc.ToolStripButton.create({
            ID: "tsGraphTreeButton", icon: "[SKIN]/actions/ai-graph-tree-icon.png", prompt: "Show Graph As Tree", autoDraw: false, showDown: false,
            click: function () {
                gvOptionsForm.setValue("is_hierarchical", "true");
                gvWindow.setTitle("Graph Visualization Window (Tree)");
                gvWindow.show();
                gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
            }
        });
        var tsMapButton = isc.ToolStripButton.create({
            ID: "tsMapButton", icon: "[SKIN]/actions/ai-maps-icon.png", prompt: "Show Map", autoDraw: false,
            click: function () { isc.say('Map viewing is not enabled for this configuration.'); }
        });
        var tsApplicationGridButton = isc.ToolStripButton.create({
            ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var windowReleaseGrid = window.releaseGrid;
                var windowAppLayout = window.appLayout;
                var releaseGrid;
                releaseGrid = windowReleaseGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.showMember(appViewGrid);
                appLayout.hideMember(releaseGrid);
            }
        });
        var tsReleaseGridButton = isc.ToolStripButton.create({
            ID: "tsReleaseGridButton", icon: "[SKIN]/actions/ai-release-number-grid-icon.png", prompt: "Release Grid", autoDraw: false,
            click: function () {
                var windowAppViewGrid = window.appViewGrid;
                var windowReleaseGrid = window.releaseGrid;
                var windowAppLayout = window.appLayout;
                var releaseGrid;
                releaseGrid = windowReleaseGrid;
                var appViewGrid;
                appViewGrid = windowAppViewGrid;
                var appLayout;
                appLayout = windowAppLayout;
                appLayout.hideMember(appViewGrid);
                appLayout.showMember(releaseGrid);
                var filterMap = new Map();
                filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                filterMap.set("_appPrefix", windowAppContext.getPrefix());
                filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                filterMap.set("_limit", windowAppContext.getCriteriaLimit());
                filterMap.set("_action", "reload");
                var simpleCriteria = {};
                filterMap.forEach(function (value, key) {
                    simpleCriteria[key] = value;
                });
                releaseGrid.invalidateCache();
                releaseGrid.filterData(simpleCriteria);
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
                    headerHTMLFlow.setContents(DataModeler.prototype.createHTMLHeader());
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
        if (windowAppContext.isModelerEnabled()) {
            if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled())) {
                commandToolStrip = isc.ToolStrip.create({
                    ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                    members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsAnalyzeButton, "separator", "starSpacer", tsSettingsButton, tsHelpButton]
                });
            }
            else {
                commandToolStrip = isc.ToolStrip.create({
                    ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                    members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsAnalyzeButton, "separator", tsGraphButton, tsGraphMatchedButton, tsGraphTreeButton, "starSpacer", tsSettingsButton, tsHelpButton]
                });
            }
        }
        else {
            commandToolStrip = isc.ToolStrip.create({
                ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                members: [fileMenuButton, "separator", "starSpacer", tsApplicationGridButton, tsReleaseGridButton, tsHelpButton]
            });
        }
        return commandToolStrip;
    };
    DataModeler.prototype.createAppViewGrid = function () {
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
                                var windowAppContext = window._appContext_;
                                if (windowAppContext.isModelerEnabled())
                                    DataModeler.prototype.executeAppViewGridSearch();
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
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled())) {
            appViewGrid = CustomListGrid.create({
                ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(), autoDraw: false,
                width: "100%", height: windowAppContext.getGridHeightPercentage(),
                autoFetchData: windowAppContext.isModelerEnabled(), showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: true,
                useAdvancedFieldPicker: true, canEditTitles: true, expansionFieldImageShowSelected: false,
                canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, canGroupBy: true, groupByMaxRecords: 1200,
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
        }
        else {
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
        }
        return appViewGrid;
    };
    DataModeler.prototype.createReleaseGrid = function () {
        var windowAppContext = window._appContext_;
        var releaseGrid = isc.ListGrid.create({
            ID: "releaseGrid", dataSource: "DM-ReleaseGrid", autoDraw: false, width: "100%",
            height: windowAppContext.getGridHeightPercentage(), autoFetchData: false, showFilterEditor: false,
            allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false, canEditTitles: false,
            expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false, leaveScrollbarGap: false,
            alternateRecordStyles: true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
            wrapCells: true, cellHeight: 50,
            getCellCSSText: function (record, rowNum, colNum) {
                if (this.getFieldName(colNum) == "release_number")
                    return "font-weight:bold; color:#000000;";
            }
        });
        return releaseGrid;
    };
    DataModeler.prototype.init = function () {
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", { multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200 });
        var windowAppContext = window._appContext_;
        var headerSection = this.createHeaderSection();
        var searchSection = this.createSearchSection();
        var commandToolStrip = this.createCommandToolStrip();
        var appViewGrid = this.createAppViewGrid();
        var releaseGrid = this.createReleaseGrid();
        if (windowAppContext.isModelerEnabled())
            this.appLayout = isc.VStack.create({
                ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                members: [headerSection, searchSection, commandToolStrip, appViewGrid]
            });
        else {
            this.appLayout = isc.VStack.create({
                ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                members: [headerSection, commandToolStrip, appViewGrid, releaseGrid]
            });
            this.appLayout.hideMember(releaseGrid);
        }
    };
    DataModeler.prototype.show = function () {
        this.appLayout.show();
    };
    DataModeler.prototype.hide = function () {
        this.appLayout.hide();
    };
    return DataModeler;
}());
//# sourceMappingURL=DataModeler.js.map