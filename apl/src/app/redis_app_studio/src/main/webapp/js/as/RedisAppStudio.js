var RedisAppStudio = (function () {
    function RedisAppStudio(anAppContext) {
        this.appContext = anAppContext;
    }
    RedisAppStudio.prototype.createHTMLHeader = function () {
        var windowAppContext = window._appContext_;
        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis Logo\" class=\"ahImage\" src=\"images/redis-small.png\" width=\"99\" height=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + windowAppContext.getAppName() + "</td>\n" +
            "  <td>&nbsp;</td> </tr> </table>";
    };
    RedisAppStudio.prototype.createHeaderSection = function () {
        var htmlString = this.createHTMLHeader();
        var headerHTMLFlow = isc.HTMLFlow.create({ ID: "headerHTMLFlow", width: "100%", height: "5%", autoDraw: false, contents: htmlString });
        return headerHTMLFlow;
    };
    RedisAppStudio.prototype.createSearchSection = function () {
        var searchForm = isc.DynamicForm.create({
            ID: "searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
            items: [{
                    type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon: true,
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
            topOperatorAppearance: "none", showSubClauseButton: false
        });
        var fbSearchButton = isc.Button.create({
            ID: "fbSearchButton", title: "Search", autoFit: true, autoDraw: false
        });
        var fbApplyButton = isc.Button.create({
            ID: "fbApplyButton", title: "Apply", autoFit: true, autoDraw: false
        });
        var fbResetButton = isc.Button.create({
            ID: "fbResetButton", title: "Reset", autoFit: true, autoDraw: false
        });
        var fbCancelButton = isc.IButton.create({
            ID: "fbCancelButton", title: "Cancel", autoFit: true, autoDraw: false
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
            ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false
        });
        var tsHighlightSearch = isc.ToolStripButton.create({
            ID: "tsHighlightSearch", icon: "[SKIN]/actions/ai-highlight-off-icon.png", prompt: "Highlight matches", showDown: false, actionType: "checkbox", showFocusOutline: false, autoDraw: false,
            click: function () {
                if (tsHighlightSearch.isSelected())
                    tsHighlightSearch.setBackgroundColor("white");
            }
        });
        var tsPhoneticSearch = isc.ToolStripButton.create({
            ID: "tsPhoneticSearch", icon: "[SKIN]/actions/ai-phonetic-off-icon_Selected.png", prompt: "Phonetic search", showDown: false, actionType: "checkbox", showFocusOutline: false, autoDraw: false,
            click: function () {
                if (tsPhoneticSearch.isSelected())
                    tsPhoneticSearch.setBackgroundColor("white");
            }
        });
        var tsExecuteSearch = isc.ToolStripButton.create({
            ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false
        });
        var tsClearSearch = isc.ToolStripButton.create({
            ID: "tsSearchClear", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Criteria", showDown: false, autoDraw: false,
            click: function () {
                tsHighlightSearch.deselect();
                tsPhoneticSearch.deselect();
            }
        });
        var tsSearch = isc.ToolStrip.create({
            ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
            members: [tsAdvancedSearch, tsHighlightSearch, tsPhoneticSearch, tsExecuteSearch, tsClearSearch]
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
                    editorType: "ComboBoxItem"
                }]
        });
        var tsClearSuggestion = isc.ToolStripButton.create({
            ID: "tsClearSuggestion", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Suggestion", showDown: false, autoDraw: false,
            click: function () {
                tsHighlightSearch.deselect();
                tsPhoneticSearch.deselect();
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
    RedisAppStudio.prototype.createCommandToolStrip = function () {
        var windowAppContext = window._appContext_;
        var docUploadForm = isc.DynamicForm.create({
            ID: "docUploadForm", width: 275, height: 75, autoDraw: false,
            fields: [
                { name: "document_title", title: "Title", type: "text", required: true },
                { name: "document_description", title: "Description", type: "text", defaultValue: "None", required: true },
                { name: "document_file", title: "File", type: "binary", required: true }
            ]
        });
        var dufSaveButton = isc.Button.create({
            ID: "dufSaveButton", title: "Upload", autoFit: true, autoDraw: false,
            click: function () {
                if (docUploadForm.valuesAreValid(false, false))
                    dufWindow.hide();
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
            ID: "documentsGrid", width: 700, height: 300, autoDraw: false, dataSource: "RS-DocumentGrid",
            autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
            alternateRecordStyles: true, showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true,
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
            ID: "dgAddButton", title: "Add", autoFit: true, autoDraw: false, disabled: true
        });
        var dgDeleteButton = isc.Button.create({
            ID: "dgDeleteButton", title: "Delete", autoFit: true, autoDraw: false
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
                        { title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-document-upload-icon.png" },
                        { title: "Manage", enabled: true, icon: "[SKIN]/actions/ai-document-manage-icon.png", click: function () {
                                documentsGrid.deselectAllRecords();
                                dgWindow.show();
                            } }
                    ] },
                { isSeparator: true },
                { title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                    submenu: [
                        { title: "Connect", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: true },
                        { title: "Delete", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: false },
                        { title: "Disconnect", icon: "[SKIN]/actions/ai-redis-disconnect-icon.png", enabled: false, checked: false }
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
                        { title: "Grid as CSV", icon: "[SKIN]/actions/ai-export-grid-csv-icon.png" },
                        { title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png" },
                        { title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png" },
                        { title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png" }
                    ] }
            ]
        });
        var fileMenuButton = isc.ToolStripMenuButton.create({
            ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
        });
        var schemaGrid = isc.ListGrid.create({
            ID: "schemaGrid", width: 710, height: 500, autoDraw: false, canEdit: true, canSort: false, alternateRecordStyles: true,
            showHeaderContextMenu: false, editEvent: "click", listEndEditAction: "next", autoSaveEdits: false
        });
        var sgApplyButton = isc.Button.create({
            ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false
        });
        var sgDiscardButton = isc.Button.create({
            ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false
        });
        var sgRebuildButton = isc.Button.create({
            ID: "sgRebuildButton", title: "Rebuild", autoFit: true, autoDraw: false
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
                sgWindow.show();
            }
        });
        var appViewForm = isc.DynamicForm.create({
            ID: "appViewForm", width: 300, height: 300, numCols: 2, autoDraw: false
        });
        var avfSaveButton = isc.Button.create({
            ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                if (appViewForm.valuesAreValid(false, false))
                    avfWindow.hide();
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
            ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false
        });
        var tsEditButton = isc.ToolStripButton.create({
            ID: "tsEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false
        });
        var tsDeleteButton = isc.ToolStripButton.create({
            ID: "tsDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false
        });
        var tsViewButton = isc.ToolStripButton.create({
            ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
            click: function () { isc.say('Document viewing is not enabled for this configuration.'); }
        });
        var detailViewer = isc.DetailViewer.create({
            ID: "detailViewer", width: 400, height: 400, autoDraw: false, showDetailFields: true
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
            ID: "tsDetailsButton", icon: "[SKIN]/actions/ai-details-icon.png", prompt: "Row Details", autoDraw: false, showDown: false
        });
        var tsMapButton = isc.ToolStripButton.create({
            ID: "tsMapButton", icon: "[SKIN]/actions/ai-maps-icon.png", prompt: "Show Map", autoDraw: false,
            click: function () { isc.say('Map viewing is not enabled for this configuration.'); }
        });
        var tsGridView = isc.DynamicForm.create({
            ID: "tsGridView", showResizeBar: false, width: 245, minWidth: 245, autoDraw: false,
            fields: [
                { name: "gridView", title: "Grid View", showTitle: true, width: "*",
                    valueMap: {
                        "application": "Application",
                        "commands": "Redis Commands"
                    },
                    defaultValue: "Application",
                    disabled: true }
            ]
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
                { name: "ds_name", title: "DS Name", type: "text", value: windowAppContext.getDSAppViewName(), canEdit: false, hint: "Data source name", wrapHintText: false },
                { name: "ds_title", title: "DS Title", type: "text", value: windowAppContext.getDSAppViewTitle(), canEdit: false, required: true, hint: "Data source title", wrapHintText: false }
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
                { name: "facet_count", title: "Facet Count", editorType: "SpinnerItem", writeStackedIcons: false, defaultValue: 10, min: 3, max: 20, step: 1 },
            ]
        });
        var settingsSaveButton = isc.Button.create({
            ID: "settingsSaveButton", title: "Save", autoFit: true, autoDraw: false,
            click: function () {
                if (setGeneralForm.valuesAreValid(false, false))
                    settingsWindow.hide();
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
            ID: "tsSettingsButton", icon: "[SKIN]/actions/ai-settings-gear-black-icon.png", prompt: "Settings", autoDraw: false
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
            members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsViewButton, tsMapButton, "separator", "starSpacer", tsGridView, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
        });
        return commandToolStrip;
    };
    RedisAppStudio.prototype.createAppViewGridLayout = function () {
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
                                else if (arrayVisibleRows[0] != -1)
                                    return isc.NumberUtil.format((arrayVisibleRows[0] + 1), "#,##0") + " to " + isc.NumberUtil.format((arrayVisibleRows[1] + 1), "#,##0") + " of " + isc.NumberUtil.format(totalRows, "#,##0");
                                else
                                    return "0 to 0 of 0";
                            }
                        }),
                        isc.LayoutSpacer.create({ width: "*" }),
                        "separator",
                        isc.ImgButton.create({
                            grid: this, src: "[SKIN]/actions/refresh.png", showRollOver: false,
                            prompt: "Refresh", width: 16, height: 16, showDown: false, autoDraw: false
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
            autoDraw: false, width: "15%", autoFetchData: false, showConnectors: true,
            showResizeBar: true, useAdvancedCriteria: false,
            data: isc.Tree.create({
                modelType: "parent", nameProperty: "facet_name",
                idField: "id", parentIdField: "parent_id",
                data: [{ id: "1", parent_id: "0", facet_name: "Facet List" }]
            }),
            fields: [
                { name: "facet_name", title: "Filter By Facets" }
            ]
        });
        var appViewGrid = CustomListGrid.create({
            ID: "appViewGrid", autoDraw: false, showFilterEditor: false, allowFilterOperators: false,
            filterOnKeypress: true, useAdvancedFieldPicker: true, canEditTitles: true,
            expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false,
            leaveScrollbarGap: false,
            fields: [
                { name: "full_name", title: "Full Name" },
                { name: "position_title", title: "Position Title" },
                { name: "office_location", title: "Office Location" },
                { name: "industry_focus", title: "Industry Focus" },
                { name: "salary", title: "Salary" },
                { name: "place_name", title: "Place Name" },
                { name: "city", title: "City" },
                { name: "state", title: "State" },
                { name: "region", title: "Region" }
            ]
        });
        var appViewGridLayout;
        if (windowAppContext.isFacetUIEnabled()) {
            appViewGrid.setWidth("85%");
            appViewGridLayout = isc.HStack.create({
                ID: "appViewGridLayout", width: "100%", height: windowAppContext.getGridHeightPercentage(), autoDraw: false,
                members: [appFacetGrid, appViewGrid]
            });
        }
        else {
            appViewGrid.setWidth("100%");
            appViewGridLayout = isc.HStack.create({
                ID: "appViewGridLayout", width: "100%", height: windowAppContext.getGridHeightPercentage(), autoDraw: false,
                members: [appViewGrid]
            });
        }
        return appViewGridLayout;
    };
    RedisAppStudio.prototype.createCommandGrid = function () {
        var windowAppContext = window._appContext_;
        var commandGrid = isc.ListGrid.create({
            ID: "commandGrid", autoDraw: false, width: "100%", height: windowAppContext.getGridHeightPercentage(),
            autoFetchData: false, showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: false,
            useAdvancedFieldPicker: false, canEditTitles: false, expansionFieldImageShowSelected: false,
            canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, wrapCells: true, cellHeight: 50,
            fields: [
                { name: "id", title: "Timestamp" },
                { name: "redis_command", title: "Command Name" },
                { name: "redis_parameters", title: "Command Parameters" },
                { name: "command_link", title: "Documentation Link" },
                { name: "command_description", title: "Description" }
            ]
        });
        return commandGrid;
    };
    RedisAppStudio.prototype.init = function () {
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
    };
    RedisAppStudio.prototype.show = function () {
        this.appLayout.show();
    };
    RedisAppStudio.prototype.hide = function () {
        this.appLayout.hide();
    };
    return RedisAppStudio;
}());
//# sourceMappingURL=RedisAppStudio.js.map