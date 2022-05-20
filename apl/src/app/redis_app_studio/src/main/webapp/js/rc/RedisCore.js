var RedisCore = (function () {
    function RedisCore(anAppContext) {
        this.appContext = anAppContext;
    }
    RedisCore.prototype.createHTMLHeader = function () {
        var windowAppContext = window._appContext_;
        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis App Studio\" class=\"ahImage\" src=\"images/redis-app-studio.svg\" height=\"99\" width=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + windowAppContext.getAppName() + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    };
    RedisCore.prototype.createHeaderSection = function () {
        var htmlString = this.createHTMLHeader();
        var headerHTMLFlow = isc.HTMLFlow.create({ ID: "headerHTMLFlow", width: "100%", height: "5%", autoDraw: false, contents: htmlString });
        return headerHTMLFlow;
    };
    RedisCore.prototype.defaultCriteria = function (aDSStructure, aDSTitle, aFetchLimit) {
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
            _redisStorageType: windowAppContext.getRedisStorageType()
        };
        return simpleCriteria;
    };
    RedisCore.prototype.executeAppViewGridExport = function (anAction, aFormat, aFetchLimit) {
        var windowAppContext = window._appContext_;
        var windowAppViewGrid = window.appViewGrid;
        var appViewGrid;
        appViewGrid = windowAppViewGrid;
        var filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_action", anAction);
        filterMap.set("_format", aFormat);
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", aFetchLimit);
        filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());
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
    RedisCore.prototype.executeAppViewGridFetch = function () {
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
    RedisCore.prototype.deleteSelectedAppViewGridRow = function (aResponse) {
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
    RedisCore.prototype.deleteSelectedDocumentsGridRow = function (aResponse) {
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
    RedisCore.prototype.deleteSelectedFlatAppViewGridRow = function (aResponse) {
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
    RedisCore.prototype.deleteSelectedHierarchyAppViewGridRow = function (aResponse) {
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
    RedisCore.prototype.rebuildDatabase = function (aResponse) {
        if (aResponse == "OK") {
            var windowSchemaGrid = window.schemaGrid;
            var schemaGrid = void 0;
            schemaGrid = windowSchemaGrid;
            if (schemaGrid != null) {
                schemaGrid.removeData(schemaGrid.data.get(0));
                isc.Notify.addMessage("Database rebuild initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    };
    RedisCore.prototype.flushDatabase = function (aResponse) {
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
                setTimeout(function () { RedisCore.prototype.executeAppViewGridFetch(); }, 2000);
            }
        }
    };
    RedisCore.prototype.updateCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    };
    RedisCore.prototype.uploadCallback = function (aDSResponse, aData, aDSRequest) {
        isc.Notify.addMessage("Upload complete", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    };
    RedisCore.prototype.dafNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataFlatForm = window.dafcUploadForm;
        var dafcUploadForm;
        dafcUploadForm = windowDataFlatForm;
        var windowDataFlatXForm = window.dafxUploadForm;
        var dafxUploadForm;
        dafxUploadForm = windowDataFlatXForm;
        dafxUploadForm.setValue("document_title", dafcUploadForm.getValue("document_title"));
        dafxUploadForm.setValue("document_description", dafcUploadForm.getValue("document_description"));
        dafxUploadForm.saveData("RedisCore.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    RedisCore.prototype.graphNextUploadCallback = function (aDSResponse, aData, aDSRequest) {
        var windowDataHierarchyNodeForm = window.graphnUploadForm;
        var graphnUploadForm;
        graphnUploadForm = windowDataHierarchyNodeForm;
        var windowDataHierarchyEdgeForm = window.grapheUploadForm;
        var grapheUploadForm;
        grapheUploadForm = windowDataHierarchyEdgeForm;
        grapheUploadForm.setValue("document_title", graphnUploadForm.getValue("document_title"));
        grapheUploadForm.setValue("document_description", graphnUploadForm.getValue("document_description"));
        grapheUploadForm.saveData("RedisCore.prototype.uploadCallback(dsResponse,data,dsRequest)");
    };
    RedisCore.prototype.createCommandToolStrip = function () {
        var windowAppContext = window._appContext_;
        var redisDBInfoForm = isc.DynamicForm.create({
            ID: "redisDBInfoForm", width: 400, height: 400, autoDraw: false, dataSource: "RC-Database", autoFetchData: false, canEdit: false
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
                                redisDBInfoForm.fetchData(RedisCore.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                });
                            } },
                        { title: "Flush DB", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: true, click: function () {
                                var windowAppContext = window._appContext_;
                                windowAppContext.assignFormContext(redisDBInfoForm);
                                redisDBInfoForm.fetchData(RedisCore.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                    redisDBInfoWindow.show();
                                    isc.confirm("Are you sure you want to flush all data?", "RedisCore.prototype.flushDatabase(value ? 'OK' : 'Cancel')");
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
                                RedisCore.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                            } },
                        { title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function () {
                                RedisCore.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                            } },
                        { title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function () {
                                RedisCore.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                            } },
                        { title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png", click: function () {
                                RedisCore.prototype.executeAppViewGridExport("command_export_txt", "txt", 100);
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
                ID: "schemaGrid", width: 710, height: 500, autoDraw: false, dataSource: "RC-SchemaGrid",
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
                ID: "schemaGrid", width: 710, height: 300, autoDraw: false, dataSource: "RC-SchemaGrid",
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
                isc.confirm("Rebuilding the database will destroy existing data - proceed with operation?", "RedisCore.prototype.rebuildDatabase(value ? 'OK' : 'Cancel')");
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
                schemaGrid.filterData(RedisCore.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
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
                    appViewForm.saveData("RedisCore.prototype.updateCallback(dsResponse,data,dsRequest)");
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
            ID: "avfWindow", title: "Redis Form Window", autoSize: true, canDragResize: true,
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
                    avfWindow.setTitle("Add Redis Form Window");
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
                    avfWindow.setTitle("Add (Duplicate) Redis Form Window");
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
                        avfWindow.setTitle("Edit Redis Form Window");
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
                        isc.confirm("Proceed with row deletion operation?", "RedisCore.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                    }
                    else
                        isc.say("You must select a row on the grid to remove.");
                }
            }
        });
        var tsViewButton = isc.ToolStripButton.create({
            ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
            click: function () { isc.say('View document selected'); }
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
            click: function () { isc.say('View Map selected'); }
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
                    var windowAppContext_1 = window._appContext_;
                    windowAppContext_1.setGroupName(setGeneralForm.getValue("app_group"));
                    windowAppContext_1.setAppName(setGeneralForm.getValue("app_name"));
                    var windowHeaderHTMLFlow = window.headerHTMLFlow;
                    var headerHTMLFlow = void 0;
                    headerHTMLFlow = windowHeaderHTMLFlow;
                    headerHTMLFlow.setContents(RedisCore.prototype.createHTMLHeader());
                    headerHTMLFlow.redraw();
                    var windowAppViewGrid = window.appViewGrid;
                    var appViewGrid = void 0;
                    appViewGrid = windowAppViewGrid;
                    if (setGridForm.getValue("column_filtering") == "Enabled")
                        appViewGrid.setShowFilterEditor(true);
                    else
                        appViewGrid.setShowFilterEditor(false);
                    if (setGridForm.getValue("csv_header") == "Title")
                        windowAppContext_1.setGridCSVHeader("title");
                    else
                        windowAppContext_1.setGridCSVHeader("field");
                    var curHighlightColor = windowAppContext_1.getHighlightFontColor();
                    var newHighlightColor = setGridForm.getValue("highlight_color");
                    if (curHighlightColor != newHighlightColor) {
                        windowAppContext_1.setHighlightFontColor(newHighlightColor);
                        windowAppContext_1.setHighlightsAssigned(false);
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
            members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsMapButton, "separator", "starSpacer", tsApplicationGridButton, tsCommandGridButton, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
        });
        return commandToolStrip;
    };
    RedisCore.prototype.createAppViewGrid = function () {
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
                                RedisCore.prototype.executeAppViewGridFetch();
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
        var appViewGrid = CustomListGrid.create({
            ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(), autoDraw: false, width: "100%",
            height: windowAppContext.getGridHeightPercentage(), autoFetchData: true, showFilterEditor: false,
            allowFilterOperators: false, filterOnKeypress: true, useAdvancedFieldPicker: true, leaveScrollbarGap: false,
            canEditTitles: true, expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false,
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
    RedisCore.prototype.createCommandGrid = function () {
        var windowAppContext = window._appContext_;
        var commandGrid = isc.ListGrid.create({
            ID: "commandGrid", dataSource: "RC-DocCmdGrid", autoDraw: false, width: "100%",
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
    RedisCore.prototype.init = function () {
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", { multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200 });
        var headerSection = this.createHeaderSection();
        var commandToolStrip = this.createCommandToolStrip();
        var appViewGrid = this.createAppViewGrid();
        var commandGrid = this.createCommandGrid();
        this.appLayout = isc.VStack.create({
            ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
            members: [headerSection, commandToolStrip, appViewGrid, commandGrid]
        });
        this.appLayout.hideMember(commandGrid);
    };
    RedisCore.prototype.show = function () {
        this.appLayout.show();
    };
    RedisCore.prototype.hide = function () {
        this.appLayout.hide();
    };
    return RedisCore;
}());
//# sourceMappingURL=RedisCore.js.map