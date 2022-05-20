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

/// <reference path="../smartclient.d.ts"/>

// noinspection HtmlDeprecatedAttribute,UnnecessaryLocalVariableJS
/**
 * The Redis App Studio Data Modeler Application is responsible for managing
 * an in-memory grid for a user who is interested in modeling their data
 * set prior to storing it in Redis. The Data Modeler class defines and
 * manages the complete SmartClient application. While this logic could
 * be broken out to other smaller files, the decision was made to keep it
 * self-contained to simplify application generation and minimize browser
 * load times.
 */
class DataModeler
{
    protected appLayout: isc.VStack;
    protected appContext: AppContext;

    constructor(anAppContext: AppContext)
    {
        this.appContext = anAppContext;
    }

    private createHTMLHeader(): string
    {
        const windowAppContext = (window as any)._appContext_;

        let appName: string;
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
    }

    // Generates the application header section of the UI
    private createHeaderSection(): isc.HTMLFlow
    {
        let htmlString = this.createHTMLHeader();
        let headerHTMLFlow = isc.HTMLFlow.create({ID:"headerHTMLFlow", width:"100%", height: "5%", autoDraw: false, contents:htmlString});

        return headerHTMLFlow;
    }

    public defaultCriteria(aDSStructure: string, aDSTitle: string, aFetchLimit?: number): any
    {
        const windowAppContext = (window as any)._appContext_;

        let fetchLimit: number;
        if (aFetchLimit)
            fetchLimit = aFetchLimit;
        else
            fetchLimit = 100;
        let simpleCriteria = {
            _dsTitle: aDSTitle,
            _dsStructure: aDSStructure,
            _appPrefix: windowAppContext.getPrefix(),
            _fetchPolicy: windowAppContext.getFetchPolicy(),
            _offset: windowAppContext.getCriteriaOffset(),
            _limit: fetchLimit
        };

        return simpleCriteria;
    }

    // Executes main grid export operations
    private executeAppViewGridExport(anAction: string, aFormat: string, aFetchLimit: number): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        const windowSearchForm = (window as any).searchForm;
        let searchForm: isc.DynamicForm;
        searchForm = windowSearchForm;
        const windowSearchFilter = (window as any).searchFilter;
        let searchFilter: isc.FilterBuilder;
        searchFilter = windowSearchFilter;
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_action", anAction);
        filterMap.set("_format", aFormat);
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", aFetchLimit);  // need to ensure page is filled with rows
        let searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        let acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        let acJSON = isc.JSON.encode(acFlattened);
        // isc.say("acJSON[" + acJSON.length + "] = " + acJSON);
        if (acJSON.length > 89)
            filterMap.set("_advancedCriteria", acJSON);
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value
        });
        let dsRequest = {
            ID: "dsRequest",
            operationId: "exportData",
            downloadResult: true,
            downloadToNewWindow: false
        };
        appViewGrid.fetchData(simpleCriteria, null, dsRequest);
        // Force a reload of the main grid after the download completes
        setTimeout(() => {  appViewGrid.filterData(simpleCriteria); }, 2000);
    }

    // Handles main grid search operations
    private executeAppViewGridSearch(): void
    {
        const minAdvancedCriteriaLength = 89;   // characters

        const windowAppContext = (window as any)._appContext_;
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", windowAppContext.getCriteriaLimit());
        const windowSearchForm = (window as any).searchForm;
        let searchForm: isc.DynamicForm;
        searchForm = windowSearchForm;
        const windowSearchFilter = (window as any).searchFilter;
        let searchFilter: isc.FilterBuilder;
        searchFilter = windowSearchFilter;
        let searchTerm = searchForm.getValue("search_terms");
        filterMap.set("_search", searchTerm);
        // Use the following logic to bypass the SC Pro license restriction
        let acFlattened = isc.DataSource.flattenCriteria(searchFilter.getCriteria());
        let acJSON = isc.JSON.encode(acFlattened);
        // isc.say("acJSON[" + acJSON.length + "] = " + acJSON);
        if (acJSON.length > minAdvancedCriteriaLength)
            filterMap.set("_advancedCriteria", acJSON);
        let simpleCriteria = {};
        filterMap.forEach((value, key) => {
            // @ts-ignore
            simpleCriteria[key] = value
        });
        const windowAppViewGrid = (window as any).appViewGrid;
        let appViewGrid: isc.ListGrid;
        appViewGrid = windowAppViewGrid;
        appViewGrid.invalidateCache();
        appViewGrid.filterData(simpleCriteria);
    }

    // Creates multiple methods for search the main grid data
    private createSearchSection(): isc.SectionStack
    {
        const windowAppContext = (window as any)._appContext_;

        // Criteria search section
        let searchForm = isc.DynamicForm.create({
                                        ID:"searchForm", autoDraw: false, iconWidth: 16, iconHeight: 16,
                                        items: [{
                                            type: "text", name: "search_terms", title: "Search Term(s)", wrapTitle: false, width: 300, suppressBrowserClearIcon:true,
                                            // @ts-ignore
                                            keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number) {
                                                if (keyName == "Enter")
                                                    DataModeler.prototype.executeAppViewGridSearch();
                                                return true;
                                            },
                                            icons: [{
                                                name: "clear", src: "[SKIN]actions/close.png", width: 10, height: 10, inline: true, inlineIconAlign: "right", prompt: "Clear Field Contents",
                                                // @ts-ignore
                                                click : function (form, item, icon) {
                                                    item.clearValue();
                                                    item.focusInItem();
                                                }
                                            }]
                                        }]
                                    });
        let searchFilter = isc.FilterBuilder.create({
                                          ID:"searchFilter", width: 500, height: 150, autoDraw: false,
                                          dataSource: windowAppContext.getAppViewDS(), topOperatorAppearance: "none",
                                          showSubClauseButton: false, criteria: {}
                                      });
        let fbSearchButton = isc.Button.create({
                                                  ID: "fbSearchButton", title: "Search", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function()
                                                  {
                                                      DataModeler.prototype.executeAppViewGridSearch();
                                                  }
                                              });
        let fbApplyButton = isc.Button.create({
                                                 ID: "fbApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                                 // @ts-ignore
                                                 click: function() {
                                                     fbWindow.hide();
                                                 }
                                             });
        let fbResetButton = isc.Button.create({
                                                  ID: "fbResetButton", title: "Reset", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      searchFilter.clearCriteria();
                                                  }
                                              });
        let fbCancelButton = isc.IButton.create({
                                                    ID: "fbCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                    // @ts-ignore
                                                    click: function() {
                                                        fbWindow.hide();
                                                    }
                                                });
        let fbButtonLayout = isc.HStack.create({
                                                   ID: "fbButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ fbSearchButton, fbApplyButton, fbResetButton, fbCancelButton ]
                                               });

        let fbFormLayout = isc.VStack.create({
                                                 ID: "fbFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ searchFilter, fbButtonLayout ]
                                             });

        let fbWindow = isc.Window.create({
                                             ID: "fbWindow", title: "Filter Builder Window", autoSize: true, autoCenter: true,
                                             isModal: false, showModalMask: false, autoDraw: false,
                                             items: [ fbFormLayout ]
                                         });
        let tsAdvancedSearch = isc.ToolStripButton.create({
                                         ID: "tsSearchAdvanced", icon: "[SKIN]/actions/ai-search-advanaced-icon.png", prompt: "Advanced Criteria Form", showDown: false, autoDraw: false,
                                          // @ts-ignore
                                          click: function()
                                          {
                                              fbWindow.show();
                                          }
                                     });
        let tsExecuteSearch = isc.ToolStripButton.create({
                                          ID: "tsSearchExecute", icon: "[SKIN]/pickers/search_picker.png", prompt: "Execute Search", showDown: false, autoDraw: false,
                                         // @ts-ignore
                                         click: function()
                                         {
                                             DataModeler.prototype.executeAppViewGridSearch();
                                         }
                                      });
        let tsClearSearch = isc.ToolStripButton.create({
                                       ID: "tsSearchClear", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Criteria", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           const windowAppViewGrid = (window as any).appViewGrid;
                                           let appViewGrid: isc.ListGrid;
                                           appViewGrid = windowAppViewGrid;
                                           const windowSuggestForm = (window as any).suggestForm;
                                           let suggestForm: isc.DynamicForm;
                                           suggestForm = windowSuggestForm;
                                           searchFilter.clearCriteria();
                                           searchForm.clearValues();
                                           suggestForm.clearValues();
                                           appViewGrid.invalidateCache();
                                           appViewGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                       }
                                     });
        let tsSearch = isc.ToolStrip.create({
                                        ID: "tsSearch", border: "0px", backgroundColor: "white", autoDraw: false,
                                         // @ts-ignore
                                         members: [tsAdvancedSearch, tsExecuteSearch, tsClearSearch]
                                     });
        let criteriaSearchLayout = isc.HStack.create({
                                 ID: "criteriaSearchLayout", width: "100%", align: "center",
                                 membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
                                 members:[ searchForm, tsSearch ]
        });

        // Auto suggestion search section
        let suggestForm = isc.DynamicForm.create({
                                                ID:"suggestForm", autoDraw: false,
                                                items: [{
                                                    name: "_suggest", title: "Suggestions", width: 300,
                                                    editorType: "ComboBoxItem", optionDataSource: "DM-SuggestList",
                                                    // @ts-ignore
                                                    pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                                    // @ts-ignore
                                                    keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number)
                                                            {
                                                                if (keyName == "Enter")
                                                                {
                                                                    const windowAppContext = (window as any)._appContext_;
                                                                    const windowAppViewGrid = (window as any).appViewGrid;
                                                                    let appViewGrid: isc.ListGrid;
                                                                    appViewGrid = windowAppViewGrid;
                                                                    let filterMap = new Map();
                                                                    filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                                                                    filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                                                                    filterMap.set("_appPrefix", windowAppContext.getPrefix());
                                                                    filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                                                                    filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                                                                    filterMap.set("_limit", 10);
                                                                    let suggestTerm = form.getValue("_suggest");
                                                                    filterMap.set("_search", suggestTerm);
                                                                    let simpleCriteria = {};
                                                                    filterMap.forEach((value, key) => {
                                                                        // @ts-ignore
                                                                        simpleCriteria[key] = value
                                                                    });
                                                                    appViewGrid.filterData(simpleCriteria);
                                                                }
                                                                return true;
                                                            }
                                                    }]
                                                });
        let tsClearSuggestion = isc.ToolStripButton.create({
                                       ID: "tsClearSuggestion", icon: "[SKIN]/pickers/clear_picker.png", prompt: "Clear Search Suggestion", showDown: false, autoDraw: false,
                                       // @ts-ignore
                                       click: function()
                                       {
                                           const windowAppViewGrid = (window as any).appViewGrid;
                                           let appViewGrid: isc.ListGrid;
                                           appViewGrid = windowAppViewGrid;
                                           const windowSuggestForm = (window as any).suggestForm;
                                           let suggestForm: isc.DynamicForm;
                                           suggestForm = windowSuggestForm;
                                           searchFilter.clearCriteria();
                                           searchForm.clearValues();
                                           suggestForm.clearValues();
                                           appViewGrid.invalidateCache();
                                           appViewGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                       }
                                   });
        let tsSuggest = isc.ToolStrip.create({
                                                 ID: "tsSuggest", border: "0px", backgroundColor: "white", autoDraw: false,
                                                members: [tsClearSuggestion]
                                            });
        let suggestedSearchLayout = isc.HStack.create({
                                          ID: "suggestedSearchLayout", width: "100%", align: "center",
                                          membersMargin: 2, layoutTopMargin: 10, layoutBottomMargin: 10,
                                         members:[ suggestForm, tsSuggest ]
                                         });
        let searchOptionsStack = isc.SectionStack.create({
                            ID: "searchOptionsStack", visibilityMode: "mutex", width: "100%", headerHeight: 23, autoDraw: false,
                            sections: [
                                {title: "Criteria Search", expanded: true, canCollapse: true, items: [ criteriaSearchLayout ]},
                                {title: "Suggested Search", expanded: false, canCollapse: true, items: [ suggestedSearchLayout ]}
                            ]
                        });

        return searchOptionsStack;
    }

    // Main grid: delete callback that generates a notification message.
    private deleteSelectedAppViewGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowAppViewGrid = (window as any).appViewGrid;
            let appViewGrid: isc.ListGrid;
            appViewGrid = windowAppViewGrid;
            if (appViewGrid != null)
            {
                appViewGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // File->Applications->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedApplicationsGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowApplicationsGrid = (window as any).applicationsGrid;
            let applicationsGrid: isc.ListGrid;
            applicationsGrid = windowApplicationsGrid;
            if (applicationsGrid != null)
            {
                applicationsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // File->Documents->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedDocumentsGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowDocumentsGrid = (window as any).documentsGrid;
            let documentsGrid: isc.ListGrid;
            documentsGrid = windowDocumentsGrid;
            if (documentsGrid != null)
            {
                documentsGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // File->Flat Data->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedFlatAppViewGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowDataFlatGrid = (window as any).dataFlatGrid;
            let dataFlatGrid: isc.ListGrid;
            dataFlatGrid = windowDataFlatGrid;
            if (dataFlatGrid != null)
            {
                dataFlatGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                // Force a reload of the manage grid after the download completes
                setTimeout(() => {  dataFlatGrid.invalidateCache(); dataFlatGrid.filterData({}); }, 2000);
            }
        }
    }

    // File->Hierarchy Data->Manage (Grid): row delete callback that generates a notification message.
    private deleteSelectedHierarchyAppViewGridRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowDataHierarchyGrid = (window as any).dataHierarchyGrid;
            let dataHierarchyGrid: isc.ListGrid;
            dataHierarchyGrid = windowDataHierarchyGrid;
            if (dataHierarchyGrid != null)
            {
                dataHierarchyGrid.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                // Force a reload of the manage grid after the download completes
                setTimeout(() => {  dataHierarchyGrid.invalidateCache(); dataHierarchyGrid.filterData({}); }, 2000);
            }
        }
    }

    // Graph relationship grid: delete callback that generates a notification message.
    private deleteSelectedGraphGridRelRow(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowGraphGridRel = (window as any).graphGridRel;
            let graphGridRel: isc.ListGrid;
            graphGridRel = windowGraphGridRel;
            if (graphGridRel != null)
            {
                graphGridRel.removeSelectedData();
                isc.Notify.addMessage("Row deleted", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
            }
        }
    }

    // Main and relationship grid: form save callback that generates a notification message.
    private updateCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });

        const windowAppContext = (window as any)._appContext_;
        if (windowAppContext.isGraphEnabled())
        {
            let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
            const windowGraphGridRel = (window as any).graphGridRel;
            let graphGridRel: isc.ListGrid;
            graphGridRel = windowGraphGridRel;
            let primaryKeyField = scDataSource.getPrimaryKeyField();
            let keyName = primaryKeyField.name;
            let simpleCriteria = {};
            // @ts-ignore
            simpleCriteria[keyName] = appViewForm.getValue(keyName);
            graphGridRel.invalidateCache();
            graphGridRel.filterData(simpleCriteria);
        }
    }

    // File->Data/Document->Upload (Form): form save callback that generates a notification message.
    private uploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Upload complete", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
    }

    // This function is called after the first data flat file has been uploaded.
    private dafNextUploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        const windowDataFlatForm = (window as any).dafcUploadForm;
        let dafcUploadForm: isc.DynamicForm;
        dafcUploadForm = windowDataFlatForm;
        const windowDataFlatXForm = (window as any).dafxUploadForm;
        let dafxUploadForm: isc.DynamicForm;
        dafxUploadForm = windowDataFlatXForm;
        dafxUploadForm.setValue("document_title", dafcUploadForm.getValue("document_title"));
        dafxUploadForm.setValue("document_description", dafcUploadForm.getValue("document_description"));
        // @ts-ignore
        dafxUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    }

    // This function is called after the first data json file has been uploaded.
    private jsonNextUploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        const windowDataHierarchyForm = (window as any).jsonfUploadForm;
        let jsonfUploadForm: isc.DynamicForm;
        jsonfUploadForm = windowDataHierarchyForm;
        const windowDataHierarchyXForm = (window as any).jsonxUploadForm;
        let jsonxUploadForm: isc.DynamicForm;
        jsonxUploadForm = windowDataHierarchyXForm;
        jsonxUploadForm.setValue("document_title", jsonfUploadForm.getValue("document_title"));
        jsonxUploadForm.setValue("document_description", jsonfUploadForm.getValue("document_description"));
        // @ts-ignore
        jsonxUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    }

    // This function is called after the graph nodes hierarchy file has been uploaded.
    private graphEdgeUploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        const windowDataHierarchyForm = (window as any).graphdUploadForm;
        let graphdUploadForm: isc.DynamicForm;
        graphdUploadForm = windowDataHierarchyForm;
        const windowDataHierarchyEdgeForm = (window as any).grapheUploadForm;
        let grapheUploadForm: isc.DynamicForm;
        grapheUploadForm = windowDataHierarchyEdgeForm;
        grapheUploadForm.setValue("document_title", graphdUploadForm.getValue("document_title"));
        grapheUploadForm.setValue("document_description", graphdUploadForm.getValue("document_description"));
        // @ts-ignore
        grapheUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
    }

    // This function is called after the first graph data hierarchy file has been uploaded.
    private graphNodeUploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        const windowDataHierarchyDataForm = (window as any).graphdUploadForm;
        let graphdUploadForm: isc.DynamicForm;
        graphdUploadForm = windowDataHierarchyDataForm;
        const windowDataHierarchyNodeForm = (window as any).graphnUploadForm;
        let graphnUploadForm: isc.DynamicForm;
        graphnUploadForm = windowDataHierarchyNodeForm;
        graphnUploadForm.setValue("document_title", graphdUploadForm.getValue("document_title"));
        graphnUploadForm.setValue("document_description", graphdUploadForm.getValue("document_description"));
        // @ts-ignore
        graphnUploadForm.saveData("DataModeler.prototype.graphEdgeUploadCallback(dsResponse,data,dsRequest)");
    }

    // File->Flat Data->Manage (Form with Generate): form save callback that generates a notification message.
    private dmGenerateCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Application ready", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
        const windowDMGenAppWindow = (window as any).dmGenAppWindow;
        let dmGenAppWindow: isc.Window;
        dmGenAppWindow = windowDMGenAppWindow;
        dmGenAppWindow.hide();
        const windowGenDMAppForm = (window as any).dmGenAppForm;
        let dmGenAppForm: isc.DynamicForm;
        dmGenAppForm = windowGenDMAppForm;
        // Allow time for the notification to complete before application launch
        setTimeout(() => {  window.open(dmGenAppForm.getValue("gen_link"), "_blank"); }, 2000);
    }

    // ToolStrip "File" Menu Item - Redis Data->Save As->RediSearch (Form with Generate): form save callback that generates a notification message.
    private rsGenerateCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Application ready", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
        const windowRSGenAppWindow = (window as any).rsGenAppWindow;
        let rsGenAppWindow: isc.Window;
        rsGenAppWindow = windowRSGenAppWindow;
        rsGenAppWindow.hide();
        const windowGenRSAppForm = (window as any).rsGenAppForm;
        let rsGenAppForm: isc.DynamicForm;
        rsGenAppForm = windowGenRSAppForm;
        // Allow time for the notification to complete before application launch
        setTimeout(() => {  window.open(rsGenAppForm.getValue("gen_link"), "_blank"); }, 2000);
    }

    private fieldPrefix(aFieldName: string): string
    {
        let fieldName = aFieldName.toLowerCase();
        let offset = fieldName.indexOf("_");
        if (offset < 2)
            return fieldName;
        else
            return fieldName.substring(0, offset);
    }

    private uniqueFormNames(aDS: isc.DataSource): Map<string,string>
    {
        let formName: string;
        let fieldPrefix: string;
        let fieldLookup: string;
        let formNames = new Map<string,string>();
        formNames.set("common", "Common");

        // @ts-ignore
        for (let fieldName of aDS.getFieldNames(false))
        {
            fieldPrefix = this.fieldPrefix(fieldName);
            fieldLookup = formNames.get(fieldPrefix);
            if (fieldLookup == undefined)
            {
                formName = fieldPrefix.charAt(0).toUpperCase() + fieldPrefix.slice(1);
                formNames.set(fieldPrefix, formName);
            }
        }

        return formNames;
    }

    private createGraphRelationshipFormLayout(): isc.VStack
    {
        const windowAppContext = (window as any)._appContext_;

        let graphEdgeFormLayout = isc.VStack.create({
                                        ID: "graphEdgeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 2
                                    });
        let graphRelForm = isc.DynamicForm.create({
                                      ID: "graphRelForm", width: 300, numCols:2, autoDraw: false, dataSource: windowAppContext.getAppViewRelDS()
                                  });
        // @ts-ignore
        graphEdgeFormLayout.addMember(graphRelForm);
        let graphEdgeVertexForm = isc.DynamicForm.create({
                                     ID: "graphEdgeVertexForm", width: 300, numCols:2, autoDraw: false,
                                     items: [{
                                         name: "_suggest", title: "End Node", editorType: "ComboBoxItem", optionDataSource: "DM-SuggestList",  required: true,
                                         // @ts-ignore
                                         pickListCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                         // @ts-ignore
                                         keyPress : function(item: isc.FormItem, form: isc.DynamicForm, keyName: isc.KeyName, characterValue: number)
                                         {
                                             if (keyName == "Enter")
                                             {
                                                 const windowGraphRelForm = (window as any).graphRelForm;
                                                 let graphRelForm: isc.DynamicForm;
                                                 graphRelForm = windowGraphRelForm;
                                                 graphRelForm.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                                             }
                                             return true;
                                         }
                                     }]
                          });
        // @ts-ignore
        graphEdgeFormLayout.addMember(graphEdgeVertexForm);
        let fiVertexName = graphRelForm.getItem("common_vertex_name");
        fiVertexName.visible = false;

        return graphEdgeFormLayout;
    }

    /* This function will decompose data source of fields into a collection of
     * form groups to aid the user with updating their vertex/node data.  This
     * logic assumes that fields follow a naming convention where common fields
     * are prefixed with "common_" and all others with their vertex labels. */
    private createGraphNodeFormLayout(aDS: isc.DataSource): isc.VStack
    {
        let uiField: any;
        let formName: string;
        let avProperties: any;
        let dynamicForm: isc.DynamicForm;
        let dsField: isc.DataSourceField;
        const windowAppContext = (window as any)._appContext_;

        let graphNodeFormLayout = isc.VStack.create({
                                ID: "graphNodeFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 5
                            });
        let formNames = this.uniqueFormNames(aDS);
        formNames.forEach((value, key) => {
            // @ts-ignore
            let uiFields = [];
            formName = key + "AppViewForm";
            avProperties = {ID: formName, width: 500, numCols:2, colWidths: [150, "*"], autoDraw: false, isGroup: true, groupTitle: value};
            for (let fieldName of aDS.getFieldNames(false))
            {
                if (fieldName.startsWith(key))
                {
                    dsField = aDS.getField(fieldName);
                    uiField = {};
                    // @ts-ignore
                    uiField["name"] = dsField.name;
                    // @ts-ignore
                    uiField["title"] = dsField.title;
                    if (fieldName.startsWith("common_"))
                    {
                        // @ts-ignore
                        uiField["required"] = "true";
                        if (fieldName == "common_vertex_label")
                        {
                            // @ts-ignore
                            uiField["hint"] = "<nobr>Node label</nobr>";
                            // @ts-ignore
                            uiField["editorType"] = "SelectItem";
                            uiField["changed"] = "DataModeler.prototype.showGraphNodeForm(value);";
                        }
                        else if (fieldName == "common_name")
                        {
                            // @ts-ignore
                            uiField["hint"] = "<nobr>Node name</nobr>";
                        }
                    }

                    // @ts-ignore
                    uiFields.add(uiField);
                }
            }
            // @ts-ignore
            avProperties["fields"] = uiFields;
            dynamicForm = isc.DynamicForm.create(avProperties);
            // @ts-ignore
            let selectNodeLabels = [];
            formNames.forEach((value, key) => {
                if (! key.startsWith("common"))
                {
                    // @ts-ignore
                    selectNodeLabels.add(value);
                }
            });
            // @ts-ignore
            dynamicForm.setValueMap("common_vertex_label", selectNodeLabels);
            // @ts-ignore
            graphNodeFormLayout.addMember(dynamicForm);
            windowAppContext.add(formName, dynamicForm);
        });

        return graphNodeFormLayout;
    }

    private graphNodeFormsClearValues(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              dynamicForm.clearValues();
                              dynamicForm.clearErrors(false);
                              dynamicForm.show();
                          });
    }

    private graphNodeFormsIsValid(aShowValid: boolean): boolean
    {
        const windowAppContext = (window as any)._appContext_;
        let dynamicForm: isc.DynamicForm;

        dynamicForm = windowAppContext.get("commonAppViewForm");
        if (aShowValid)
            dynamicForm.validate(false);
        else if (! dynamicForm.valuesAreValid(false, false))
            return false;

        return true;
    }

    private appViewToGraphNodeForms(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let appViewForm: isc.DynamicForm;
        appViewForm = windowAppViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              dynamicForm.clearValues();
                              for (let formItem of dynamicForm.fields)
                                  dynamicForm.setValue(formItem.name, appViewForm.getValue(formItem.name));
                          });
    }

    private graphNodesToAppViewForm(): void
    {
        const windowAppContext = (window as any)._appContext_;
        const windowAppViewForm = (window as any).appViewForm;
        let appViewForm: isc.DynamicForm;
        appViewForm = windowAppViewForm;
        let dynamicForm: isc.DynamicForm;

        let formName: string;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        let formNames = this.uniqueFormNames(scDataSource);
        formNames.forEach((value, key) =>
                          {
                              formName = key + "AppViewForm";
                              dynamicForm = windowAppContext.get(formName);
                              for (let formItem of dynamicForm.fields)
                                  appViewForm.setValue(formItem.name, dynamicForm.getValue(formItem.name));
                          });
    }

    private showGraphNodeForm(aLabelName: string): void
    {
        if (aLabelName != undefined)
        {
            const windowAppContext = (window as any)._appContext_;
            let dynamicForm: isc.DynamicForm;

            let formName: string;
            let formLabelPrefix = aLabelName.toLowerCase();
            let selectedFormName = formLabelPrefix + "AppViewForm";
            let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
            let formNames = this.uniqueFormNames(scDataSource);
            formNames.forEach((value, key) =>
                  {
                      formName = key + "AppViewForm";
                      dynamicForm = windowAppContext.get(formName);
                      if (formName === selectedFormName)
                          dynamicForm.show();
                      else if (formName != "commonAppViewForm")
                          dynamicForm.hide();
                  });
        }
    }

    private graphVisualizationOptionsToURL(isDownload: boolean): string
    {
        const windowAppContext = (window as any)._appContext_;
        const windowGVOptionsForm = (window as any).gvOptionsForm;
        let gvOptionsForm: isc.DynamicForm;
        gvOptionsForm = windowGVOptionsForm;

        let gvURL = windowAppContext.getGraphVisualizationURL() + "?";
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

        let gvEncodedURL = gvURL.replace(/#/g, "%23");

        isc.logWarn("gvEncodedURL = " + gvEncodedURL);

        return gvEncodedURL;
    }

    // Create the ToolStrip component with command buttons
    private createCommandToolStrip(): isc.ToolStrip
    {
        const windowAppContext = (window as any)._appContext_;

        // ToolStrip "File" Menu Item - Data flat (csv,xml) application generate form
        let dmGenAppForm = isc.DynamicForm.create({
                                ID: "dmGenAppForm", autoDraw: false, width: 500, colWidths: [190, "*"],dataSource: "DM-GenAppForm",
                                fields: [
                                    {name: "app_group", title:"App Group", type:"text", value: "Redis App Studio", required: true, hint: "Application group", wrapHintText: false},
                                    {name: "app_name", title:"App Name", type:"text", defaultValue: "Data Modeler", required: true, hint: "Name of application", wrapHintText: false},
                                    {name: "app_prefix", title:"App Prefix", type:"text", canEdit: true, hint: "Application prefix (3 characters)", wrapHintText: false},
                                    {name: "app_type", title:"App Type", type:"text", value: "Data Modeler", editorType: "ComboBoxItem", canEdit: true, hint: "Application type", wrapHintText: false},
                                    {name: "ds_structure", title:"DS Structure", type:"text",  canEdit: true, editorType: "SelectItem", hint: "Data source type selected", wrapHintText: false, wrapTitle: false},
                                    {name: "ds_title", title:"DS Title", type:"text",  canEdit: false, hint: "Data source title selected", wrapHintText: false},
                                    // @ts-ignore
                                    {name: "grid_height", title: "Grid Height", editorType: "SpinnerItem", writeStackedIcons: false, hint: "Percentage of page for grid", wrapHintText: false, wrapTitle: false, defaultValue: windowAppContext.getGridHeightNumber(), min: 30, max: 100, step: 5},
                                    {name: "skin_name", title:"UI Theme", type:"text", value: "Tahoe", editorType: "ComboBoxItem", canEdit: true, hint: "UI styling theme", wrapHintText: false}
                                ]
                            });
        let dmGenAppCreateButton = isc.Button.create({
                                       ID: "dmGenAppCreateButton", title: "Create", autoFit: true, autoDraw: false,
                                       // @ts-ignore
                                       click: function() {
                                           if (dmGenAppForm.valuesAreValid(false, false))
                                           {
                                               let appPrefix = dmGenAppForm.getValue("app_prefix");
                                               if ((! appPrefix) || (appPrefix.length != 3))
                                                   isc.warn("Application prefix must be 3 characters in length.");
                                               else
                                               {
                                                   let appPrefixUC = appPrefix.toUpperCase();
                                                   dmGenAppForm.setValue("app_prefix", appPrefixUC);
                                                   // @ts-ignore
                                                   dmGenAppForm.saveData("DataModeler.prototype.dmGenerateCallback(dsResponse,data,dsRequest)");

                                               }
                                           }
                                           else
                                               dmGenAppForm.validate(false);
                                       }
                                   });
        let dmGenAppCancelButton = isc.IButton.create({
                                                          ID: "dmGenAppCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                          // @ts-ignore
                                                          click: function() {
                                                              dmGenAppWindow.hide();
                                                          }
                                                      });
        let dmGenAppButtonLayout = isc.HStack.create({
                                                         ID: "dmGenAppButtonLayout", width: "100%", height: 24,
                                                         layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                                         members: [ dmGenAppCreateButton, dmGenAppCancelButton ]
                                                     });
        let dmGenAppFormLayout = isc.VStack.create({
                                                       ID: "dmGenAppFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                       layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                       members:[ dmGenAppForm, dmGenAppButtonLayout ]
                                                   });
        let dmGenAppWindow = isc.Window.create({
                                                   ID: "dmGenAppWindow", title: "Generate Application Window", autoSize: true, autoCenter: true,
                                                   isModal: true, showModalMask: true, autoDraw: false,
                                                   items: [ dmGenAppFormLayout ]
                                               });
        // ToolStrip "File" Menu Item - Data flat (csv,xml) upload form and grid
        let dafcUploadForm = isc.DynamicForm.create({
                                                       ID: "dafcUploadForm", width: 275, height: 50, autoDraw: false,
                                                       dataSource: "DM-DataFlatGrid",
                                                       fields: [
                                                           {name: "document_title", title:"Title", type:"text", required: true},
                                                           {name: "document_description", title:"Description", type:"text", defaultValue: "None", required: true},
                                                           {name: "document_file", title:"Data File", type:"binary", wrapTitle: false, required: true}
                                                       ]
                                                   });
        let dafxUploadForm = isc.DynamicForm.create({
                                                        ID: "dafxUploadForm", width: 275, height: 20, autoDraw: false,
                                                        dataSource: "DM-DataFlatGrid",
                                                        fields: [
                                                            {name: "document_file", title:"Schema File", type:"binary", wrapTitle: false, required: true}
                                                        ]
                                                    });
        let dafcxFormLayout = isc.VStack.create({
                                                        ID: "dafcxFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
                                                        members:[ dafcUploadForm, dafxUploadForm ]
                                                    });
        let dafSaveButton = isc.Button.create({
                                                  ID: "dafSaveButton", title: "Upload", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      if ((dafcUploadForm.valuesAreValid(false, false)) &&
                                                          (dafxUploadForm.valuesAreValid(false, false)))
                                                      {
                                                          let csvFileName: string;
                                                          csvFileName = dafcUploadForm.getValue("document_file");
                                                          let xmlFileName: string;
                                                          xmlFileName = dafxUploadForm.getValue("document_file");
                                                          if (! csvFileName.toLowerCase().endsWith(".csv"))
                                                              isc.say("Data file name must end with a 'csv' extension");
                                                          else if (! xmlFileName.toLowerCase().endsWith(".xml"))
                                                              isc.say("Schema file name must end with a 'xml' extension");
                                                          else
                                                          {
                                                              // @ts-ignore
                                                              dafcUploadForm.saveData("DataModeler.prototype.dafNextUploadCallback(dsResponse,data,dsRequest)");
                                                              dafWindow.hide();
                                                          }
                                                      }
                                                      else
                                                      {
                                                          dafcUploadForm.validate(false);
                                                          dafxUploadForm.validate(false);
                                                      }
                                                  }
                                              });
        let dafCancelButton = isc.IButton.create({
                                                     ID: "dafCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         dafWindow.hide();
                                                     }
                                                 });
        let dafButtonLayout = isc.HStack.create({
                                                    ID: "dafButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                    autoDraw: false, membersMargin: 40,
                                                    members: [ dafSaveButton, dafCancelButton ]
                                                });
        let dafUploadFormLayout = isc.VStack.create({
                                                        ID: "dafUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                        layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                        members:[ dafcxFormLayout, dafButtonLayout ]
                                                    });
        let dafWindow = isc.Window.create({
                                              ID: "dafWindow", title: "Flat Data Upload Form Window", autoSize: true,
                                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                              items: [ dafUploadFormLayout ]
                                          });

        let applicationsGrid = isc.ListGrid.create({
                                                   ID:"applicationsGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-ApplicationGrid",
                                                   autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                                   alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                                   showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
                                                   cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
                                                   // @ts-ignore
                                                   recordDoubleClick: function () {
                                                       let lgRecord = applicationsGrid.getSelectedRecord();
                                                       if (lgRecord != null)
                                                       {
                                                           // @ts-ignore
                                                           setTimeout(() => {  window.open(lgRecord.document_link, "_blank"); }, 100);
                                                       }
                                                   },
                                                   fields:[
                                                       {name:"document_name", title:"Name", width:200 },
                                                       {name:"document_title", title:"Title", width:200},
                                                       {name:"document_type", title:"Type", width:75},
                                                       {name:"document_description", title:"Description", width:200},
                                                       {name:"document_date", title:"Upload Date", width:100},
                                                       {name:"document_owner", title:"Owner", width:75}
                                                   ]
                                               });
        let ragDeleteButton = isc.Button.create({
                                                   ID: "ragDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       let lgRecord = applicationsGrid.getSelectedRecord();
                                                       if (lgRecord != null)
                                                       {
                                                           // @ts-ignore
                                                           if (lgRecord.document_owner != "System")
                                                               // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                               isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedApplicationsGridRow(value ? 'OK' : 'Cancel')");
                                                           else
                                                               isc.say("You cannot delete a 'System' owned application.");
                                                       }
                                                       else
                                                           isc.say("You must select a row on the grid to remove.");
                                                   }
                                               });
        let ragCloseButton = isc.IButton.create({
                                                   ID: "ragCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       ragWindow.hide();
                                                   }
                                               });
        let ragButtonLayout = isc.HStack.create({
                                                   ID: "ragButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ ragDeleteButton, ragCloseButton ]
                                               });
        let ragFormLayout = isc.VStack.create({
                                                 ID: "ragFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ applicationsGrid, ragButtonLayout ]
                                             });
        let ragWindow = isc.Window.create({
                                             ID: "ragWindow", title: "Applications Manager Window", autoSize: true,
                                             autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ ragFormLayout ]
                                         });

        let dataFlatGrid = isc.ListGrid.create({
                                                   ID:"dataFlatGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DataFlatGrid",
                                                   autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                                   alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                                   showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
                                                   cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
                                                   fields:[
                                                        {name:"document_name", title:"Name", width:200 },
                                                        {name:"document_title", title:"Title", width:200},
                                                        {name:"document_type", title:"Type", width:75},
                                                        {name:"document_description", title:"Description", width:200},
                                                        {name:"document_date", title:"Upload Date", width:100},
                                                        {name:"document_size", title:"File Size", width:75}
                                                    ]
                                                });
        let dafgGenerateButton = isc.Button.create({
                                                ID: "dafgGenerateButton", title: "Generate ...", autoFit: true, autoDraw: false, disabled: false,
                                                // @ts-ignore
                                                click: function() {
                                                    let lgRecord = dataFlatGrid.getSelectedRecord();
                                                    if (lgRecord != null)
                                                    {
                                                        dmGenAppForm.clearErrors(false);
                                                        dmGenAppForm.clearValues();
                                                        dmGenAppForm.setValue("ds_structure", "Flat");
                                                        // @ts-ignore
                                                        dmGenAppForm.setValue("ds_title", lgRecord.document_title);
                                                        dmGenAppForm.setValue("grid_height", "80");
                                                        dmGenAppWindow.show();
                                                    }
                                                    else
                                                        isc.say("You must select a row to generate application.");
                                                }
                                            });
        let dafgDeleteButton = isc.Button.create({
                                                   ID: "dafgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       let lgRecord = dataFlatGrid.getSelectedRecord();
                                                       if (lgRecord != null)
                                                       {
                                                           // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                           isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedFlatAppViewGridRow(value ? 'OK' : 'Cancel')");
                                                       }
                                                       else
                                                           isc.say("You must select a row on the grid to remove.");
                                                   }
                                               });
        let dafgCloseButton = isc.IButton.create({
                                                   ID: "dafgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       dafgWindow.hide();
                                                   }
                                               });
        let dafgButtonLayout = isc.HStack.create({
                                                   ID: "dafgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ dafgGenerateButton, dafgDeleteButton, dafgCloseButton ]
                                               });
        let dafgFormLayout = isc.VStack.create({
                                                 ID: "dafgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ dataFlatGrid, dafgButtonLayout ]
                                             });
        let dafgWindow = isc.Window.create({
                                             ID: "dafgWindow", title: "Flat Data Manager Window", autoSize: true,
                                             autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ dafgFormLayout ]
                                         });

        // ToolStrip "File" Menu Item - Data hierarchy (json) upload form and grid
        let jsonfUploadForm = isc.DynamicForm.create({
                                                       ID: "jsonfUploadForm", width: 275, height: 75, autoDraw: false,
                                                       dataSource: "DM-DataHierJSON",
                                                       fields: [
                                                           {name: "document_title", title:"Title", type:"text", required: true},
                                                           {name: "document_description", title:"Description", type:"text", defaultValue: "None", required: true},
                                                           {name: "document_file", title:"JSON File", type:"binary", required: true}
                                                       ]
                                                   });
        let jsonxUploadForm = isc.DynamicForm.create({
                                                        ID: "jsonxUploadForm", width: 275, height: 20, autoDraw: false,
                                                        dataSource: "DM-DataHierJSON",
                                                        fields: [
                                                            {name: "document_file", title:"Schema File", type:"binary", wrapTitle: false, required: true}
                                                        ]
                                                    });
        let jsonfxFormLayout = isc.VStack.create({
                                                    ID: "jsonfxFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
                                                    members:[ jsonfUploadForm, jsonxUploadForm ]
                                                });
        let jsonSaveButton = isc.Button.create({
                                                  ID: "jsonSaveButton", title: "Upload", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      if ((jsonfUploadForm.valuesAreValid(false, false)) &&
                                                          (jsonxUploadForm.valuesAreValid(false, false)))
                                                      {
                                                          let jsonDataFileName: string;
                                                          jsonDataFileName = jsonfUploadForm.getValue("document_file");
                                                          let xmlSchemaFileName: string;
                                                          xmlSchemaFileName = jsonxUploadForm.getValue("document_file");
                                                          if (! jsonDataFileName.toLowerCase().endsWith(".json"))
                                                              isc.say("JSON data file name must end with a 'json' extension");
                                                          else if (! xmlSchemaFileName.toLowerCase().endsWith(".xml"))
                                                              isc.say("Schema file name must end with a 'xml' extension");
                                                          else
                                                          {
                                                              // @ts-ignore
                                                              jsonfUploadForm.saveData("DataModeler.prototype.jsonNextUploadCallback(dsResponse,data,dsRequest)");
                                                              jsonWindow.hide();
                                                          }
                                                      }
                                                      else
                                                      {
                                                          jsonfUploadForm.validate(false);
                                                          jsonxUploadForm.validate(false);
                                                      }
                                                  }
                                              });
        let jsonCancelButton = isc.IButton.create({
                                                     ID: "jsonCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         jsonWindow.hide();
                                                     }
                                                 });
        let jsonButtonLayout = isc.HStack.create({
                                                    ID: "jsonButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                    autoDraw: false, membersMargin: 40,
                                                    members: [ jsonSaveButton, jsonCancelButton ]
                                                });
        let jsonUploadFormLayout = isc.VStack.create({
                                                        ID: "jsonUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                        layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                        members:[ jsonfxFormLayout, jsonButtonLayout ]
                                                    });
        let jsonWindow = isc.Window.create({
                                              ID: "jsonWindow", title: "JSON Upload Form Window", autoSize: true,
                                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                              items: [ jsonUploadFormLayout ]
                                          });

        // ToolStrip "File" Menu Item - Data hierarchy (graph) upload form and grid
        let graphdUploadForm = isc.DynamicForm.create({
                                                        ID: "graphdUploadForm", width: 275, height: 50, autoDraw: false,
                                                        dataSource: "DM-DataHierGraph",
                                                        fields: [
                                                            {name: "document_title", title:"Title", type:"text", required: true},
                                                            {name: "document_description", title:"Description", type:"text", defaultValue: "None", required: true},
                                                            {name: "document_file", title:"Graph Data", type:"binary", wrapTitle: false, required: true}
                                                        ]
                                                    });
        let graphnUploadForm = isc.DynamicForm.create({
                                                          ID: "graphnUploadForm", width: 275, height: 20, autoDraw: false,
                                                          dataSource: "DM-DataHierGraph",
                                                          fields: [
                                                              {name: "document_file", title:"Graph Nodes", type:"binary", wrapTitle: false, required: true}
                                                          ]
                                                      });
        let grapheUploadForm = isc.DynamicForm.create({
                                                        ID: "grapheUploadForm", width: 275, height: 20, autoDraw: false,
                                                        dataSource: "DM-DataHierGraph",
                                                        fields: [
                                                            {name: "document_file", title:"Graph Edges", type:"binary", wrapTitle: false, required: true}
                                                        ]
                                                    });
        let graphdneFormLayout = isc.VStack.create({
                                                    ID: "graphdneFormLayout", width: "100%", align: "center", autoDraw: false, membersMargin: 1,
                                                    members:[ graphdUploadForm, graphnUploadForm, grapheUploadForm ]
                                                });
        let graphSaveButton = isc.Button.create({
                                                  ID: "graphSaveButton", title: "Upload", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      if ((graphdUploadForm.valuesAreValid(false, false)) &&
                                                          (graphnUploadForm.valuesAreValid(false, false)) &&
                                                          (grapheUploadForm.valuesAreValid(false, false)))
                                                      {
                                                          let csvDataFileName: string;
                                                          csvDataFileName = graphdUploadForm.getValue("document_file");
                                                          let xmlNodesFileName: string;
                                                          xmlNodesFileName = graphnUploadForm.getValue("document_file");
                                                          let xmlEdgesFileName: string;
                                                          xmlEdgesFileName = grapheUploadForm.getValue("document_file");
                                                          if (! csvDataFileName.toLowerCase().endsWith(".csv"))
                                                              isc.say("Graph data file name must end with a 'csv' extension");
                                                          else if (! xmlNodesFileName.toLowerCase().endsWith(".xml"))
                                                              isc.say("Graph nodes file name must end with a 'xml' extension");
                                                          else if (! xmlEdgesFileName.toLowerCase().endsWith(".xml"))
                                                              isc.say("Graph edges file name must end with a 'xml' extension");
                                                          else
                                                          {
                                                              // @ts-ignore
                                                              graphdUploadForm.saveData("DataModeler.prototype.graphNodeUploadCallback(dsResponse,data,dsRequest)");
                                                              graphWindow.hide();
                                                          }
                                                      }
                                                      else
                                                      {
                                                          graphdUploadForm.validate(false);
                                                          graphnUploadForm.validate(false);
                                                          grapheUploadForm.validate(false);
                                                      }
                                                  }
                                              });
        let graphCancelButton = isc.IButton.create({
                                                     ID: "graphCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         graphWindow.hide();
                                                     }
                                                 });
        let graphButtonLayout = isc.HStack.create({
                                                    ID: "graphButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                    autoDraw: false, membersMargin: 40,
                                                    members: [ graphSaveButton, graphCancelButton ]
                                                });
        let graphUploadFormLayout = isc.VStack.create({
                                                        ID: "graphUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                        layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                        members:[ graphdneFormLayout, graphButtonLayout ]
                                                    });
        let graphWindow = isc.Window.create({
                                              ID: "graphWindow", title: "Graph Data Upload Form Window", autoSize: true,
                                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                              items: [ graphUploadFormLayout ]
                                          });
        let dataHierarchyGrid = isc.ListGrid.create({
                                                    ID:"dataHierarchyGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DataHierGraph",
                                                    autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                                    alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                                    showHeaderContextMenu: false, autoSaveEdits: false, canEdit: false, wrapCells: true,
                                                    cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
                                                    fields:[
                                                       {name:"document_name", title:"Name", width:200},
                                                       {name:"document_title", title:"Title", width:200},
                                                       {name:"document_type", title:"Type", width:100},
                                                       {name:"document_description", title:"Description", width:200},
                                                       {name:"document_date", title:"Upload Date", width:100},
                                                       {name:"document_size", title:"File Size", width:75}
                                                   ]
                                               });
        let dahgGenerateButton = isc.Button.create({
                                                       ID: "dahgGenerateButton", title: "Generate", autoFit: true, autoDraw: false, disabled: false,
                                                       // @ts-ignore
                                                       click: function() {
                                                           let lgRecord = dataHierarchyGrid.getSelectedRecord();
                                                           if (lgRecord != null)
                                                           {
                                                               dmGenAppForm.clearErrors(false);
                                                               dmGenAppForm.clearValues();
                                                               dmGenAppForm.setValue("ds_structure", "Hierarchy");
                                                               // @ts-ignore
                                                               dmGenAppForm.setValue("ds_title", lgRecord.document_title);
                                                               dmGenAppForm.setValue("grid_height", "80");
                                                               dmGenAppWindow.show();
                                                           }
                                                           else
                                                               isc.say("You must select a row to generate application.");
                                                       }
                                                   });
        let dahgDeleteButton = isc.Button.create({
                                                     ID: "dahgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         let lgRecord = dataHierarchyGrid.getSelectedRecord();
                                                         if (lgRecord != null)
                                                         {
                                                             // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                             isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedHierarchyAppViewGridRow(value ? 'OK' : 'Cancel')");
                                                         }
                                                         else
                                                             isc.say("You must select a row on the grid to remove.");
                                                     }
                                                 });
        let dahgCloseButton = isc.IButton.create({
                                                     ID: "dahgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         dahgWindow.hide();
                                                     }
                                                 });
        let dahgButtonLayout = isc.HStack.create({
                                                     ID: "dahgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                     autoDraw: false, membersMargin: 40,
                                                     members: [ dahgGenerateButton, dahgDeleteButton, dahgCloseButton ]
                                                 });
        let dahgFormLayout = isc.VStack.create({
                                                   ID: "dahgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                   layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                   members:[ dataHierarchyGrid, dahgButtonLayout ]
                                               });
        let dahgWindow = isc.Window.create({
                                               ID: "dahgWindow", title: "Hierarchy Data Manager Window", autoSize: true,
                                               autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                               items: [ dahgFormLayout ]
                                           });

        // ToolStrip "File" Menu Item - Document upload form and grid
        let docUploadForm = isc.DynamicForm.create({
                                                      ID: "docUploadForm", width: 275, height: 75, autoDraw: false,
                                                      dataSource: "DM-DocumentGrid",
                                                       fields: [
                                                           {name: "document_title", title:"Title", type:"text", required: true},
                                                           {name: "document_description", title:"Description", type:"text", defaultValue: "None", required: true},
                                                           {name: "document_file", title:"File", type:"binary", required: true}
                                                       ]
                                                  });
        let dufSaveButton = isc.Button.create({
                                                  ID: "dufSaveButton", title: "Upload", autoFit: true, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function() {
                                                      if (docUploadForm.valuesAreValid(false, false))
                                                      {
                                                          // @ts-ignore
                                                          docUploadForm.saveData("DataModeler.prototype.uploadCallback(dsResponse,data,dsRequest)");
                                                          dufWindow.hide();
                                                      }
                                                      else
                                                          docUploadForm.validate(false);
                                                  }
                                              });
        let dufCancelButton = isc.IButton.create({
                                                     ID: "dufCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         dufWindow.hide();
                                                     }
                                                 });
        let dufButtonLayout = isc.HStack.create({
                                                    ID: "dufButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                    autoDraw: false, membersMargin: 40,
                                                    members: [ dufSaveButton, dufCancelButton ]
                                                });
        let docUploadFormLayout = isc.VStack.create({
                                                       ID: "docUploadFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                       layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                       members:[ docUploadForm, dufButtonLayout ]
                                                   });
        let dufWindow = isc.Window.create({
                                              ID: "dufWindow", title: "Document Upload Form Window", autoSize: true,
                                              autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                              items: [ docUploadFormLayout ]
                                          });
        let documentsGrid = isc.ListGrid.create({
                                                 ID:"documentsGrid", width: 700, height: 300, autoDraw: false, dataSource: "DM-DocumentGrid",
                                                 autoFetchData: true, canRemoveRecords: false, canSort: false, autoFitData: "horizontal",
                                                 alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                                 showHeaderContextMenu: false, autoSaveEdits: true, canEdit: true, wrapCells: true,
                                                 cellHeight: 36, editEvent: "doubleClick", listEndEditAction: "done",
                                                 fields:[
                                                     {name:"document_name", title:"Name", width:200},
                                                     {name:"document_title", title:"Title", width:200},
                                                     {name:"document_type", title:"Type", width:75},
                                                     {name:"document_description", title:"Description", width:200},
                                                     {name:"document_date", title:"Upload Date", width:100},
                                                     {name:"document_size", title:"File Size", width:75}
                                                 ]
                                             });
        let dgAddButton = isc.Button.create({
                                                  ID: "dgAddButton", title: "Add", autoFit: true, autoDraw: false, disabled: true,
                                                  // @ts-ignore
                                                  click: function() {
                                                      isc.say("Add document button pressed");
                                                  }
                                              });
        let dgDeleteButton = isc.Button.create({
                                                ID: "dgDeleteButton", title: "Delete", autoFit: true, autoDraw: false,
                                                // @ts-ignore
                                                click: function() {
                                                    let lgRecord = documentsGrid.getSelectedRecord();
                                                    if (lgRecord != null)
                                                    {
                                                        // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                        isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedDocumentsGridRow(value ? 'OK' : 'Cancel')");
                                                    }
                                                    else
                                                        isc.say("You must select a row on the grid to remove.");
                                                }
                                            });
        let dgCloseButton = isc.IButton.create({
                                                   ID: "dgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                   // @ts-ignore
                                                   click: function() {
                                                       dgWindow.hide();
                                                   }
                                               });
        let dgButtonLayout = isc.HStack.create({
                                                   ID: "dgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                                   autoDraw: false, membersMargin: 40,
                                                   members: [ dgAddButton, dgDeleteButton, dgCloseButton ]
                                               });
        let dgFormLayout = isc.VStack.create({
                                                 ID: "dgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                 members:[ documentsGrid, dgButtonLayout ]
                                             });
        let dgWindow = isc.Window.create({
                                             ID: "dgWindow", title: "Documents Manager Window", autoSize: true,
                                             autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ dgFormLayout ]
                                         });

        // ToolStrip "File" Menu Item - Redis Data->Save As->RediSearch application generate form
        let rsGenAppForm = isc.DynamicForm.create({
                                                    ID: "rsGenAppForm", autoDraw: false, width: 500, colWidths: [190, "*"],dataSource: "DM-GenAppForm",
                                                    fields: [
                                                        {name: "app_group", title:"App Group", type:"text", value: "Redis App Studio", required: true, hint: "Application group", wrapHintText: false},
                                                        {name: "app_name", title:"App Name", type:"text", required: true, hint: "Name of application", wrapHintText: false},
                                                        {name: "app_prefix", title:"App Prefix", type:"text", canEdit: true, hint: "Application prefix (3 characters)", wrapHintText: false},
                                                        {name: "app_type", title:"App Type", type:"text", editorType: "ComboBoxItem", canEdit: true, hint: "Application type", wrapHintText: false},
                                                        {name: "ds_structure", title:"DS Structure", type:"text",  canEdit: true, editorType: "SelectItem", hint: "Data source type selected", wrapHintText: false, wrapTitle: false},
                                                        {name: "ds_title", title:"DS Title", type:"text",  canEdit: false, hint: "Data source title selected", wrapHintText: false},
                                                        // @ts-ignore
                                                        {name: "grid_height", title: "Grid Height", editorType: "SpinnerItem", writeStackedIcons: false, hint: "Percentage of page for grid", wrapHintText: false, wrapTitle: false, defaultValue: windowAppContext.getGridHeightNumber(), min: 30, max: 100, step: 5},
                                                        {name: "rc_storage_type", title:"Storage Type", type:"text", editorType: "ComboBoxItem", canEdit: true, hint: "Row storage type", wrapHintText: false},
                                                        // @ts-ignore
                                                        {name: "ui_facets", title: "UI Facets", type: "radioGroup", defaultValue: "Disabled", valueMap: ["Disabled", "Enabled"], vertical:false},
                                                        {name: "skin_name", title:"UI Theme", type:"text", value: "Tahoe", editorType: "ComboBoxItem", canEdit: true, hint: "UI styling theme", wrapHintText: false}
                                                    ]
                                                });
        let rsGenAppCreateButton = isc.Button.create({
                                                       ID: "rsGenAppCreateButton", title: "Create", autoFit: true, autoDraw: false,
                                                       // @ts-ignore
                                                       click: function() {
                                                           if (rsGenAppForm.valuesAreValid(false, false))
                                                           {
                                                               let appPrefix = rsGenAppForm.getValue("app_prefix");
                                                               if ((! appPrefix) || (appPrefix.length != 3))
                                                                   isc.warn("Application prefix must be 3 characters in length.");
                                                               else
                                                               {
                                                                   let appPrefixUC = appPrefix.toUpperCase();
                                                                   rsGenAppForm.setValue("app_prefix", appPrefixUC);
                                                                   // @ts-ignore
                                                                   rsGenAppForm.saveData("DataModeler.prototype.rsGenerateCallback(dsResponse,data,dsRequest)");

                                                               }
                                                           }
                                                           else
                                                               rsGenAppForm.validate(false);
                                                       }
                                                   });
        let rsGenAppCancelButton = isc.IButton.create({
                                                        ID: "rsGenAppCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                                        // @ts-ignore
                                                        click: function() {
                                                            rsGenAppWindow.hide();
                                                        }
                                                    });
        let rsGenAppButtonLayout = isc.HStack.create({
                                                       ID: "rsGenAppButtonLayout", width: "100%", height: 24,
                                                       layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                                       members: [ rsGenAppCreateButton, rsGenAppCancelButton ]
                                                   });
        let rsGenAppFormLayout = isc.VStack.create({
                                                     ID: "rsGenAppFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                     layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                                     members:[ rsGenAppForm, rsGenAppButtonLayout ]
                                                 });
        let rsGenAppWindow = isc.Window.create({
                                                 ID: "rsGenAppWindow", title: "Generate Application Window", autoSize: true, autoCenter: true,
                                                 isModal: true, showModalMask: true, autoDraw: false,
                                                 items: [ rsGenAppFormLayout ]
                                             });
        let fileMenu : isc.Menu;
        if (windowAppContext.isModelerEnabled())
        {
            // ToolStrip "File" menu for fully enabled
            fileMenu = isc.Menu.create({
                               ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                               data: [
                                   {title: "Applications ...", icon: "[SKIN]/actions/ai-application-commands-icon.png",
                                       submenu: [
                                           {title: "Manage", icon: "[SKIN]/actions/ai-application-manage-icon.png", enabled: true, click: function() {
                                                   applicationsGrid.deselectAllRecords();
                                                   ragWindow.show();
                                               }}
                                       ]},
                                   {isSeparator: true},
                                   {title: "Document ...", icon: "[SKIN]/actions/ai-document-commands-icon.png",
                                       submenu: [
                                           {title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-document-upload-icon.png", click: function() {
                                                   docUploadForm.clearValues();
                                                   dufWindow.show();
                                               }},
                                           {title: "Manage", enabled: true, icon: "[SKIN]/actions/ai-document-manage-icon.png", click: function() {
                                                   documentsGrid.deselectAllRecords();
                                                   dgWindow.show();
                                               }}
                                       ]},
                                   {isSeparator: true},
                                   {title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                                       submenu: [
                                           {title: "Save As ...", icon: "[SKIN]/actions/ai-redis-save-as-icon.png", enabled: true, submenu: [
                                                   {title: "RedisCore", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isStructureFlat(), click: function() {
                                                           const windowAppContext = (window as any)._appContext_;
                                                           rsGenAppForm.clearErrors(false);
                                                           rsGenAppForm.clearValue("app_prefix");
                                                           rsGenAppForm.setValue("app_name", "Redis Application");
                                                           rsGenAppForm.setValue("app_type", "Redis Core");
                                                           rsGenAppForm.setValue("ds_structure", "Flat");
                                                           // @ts-ignore
                                                           rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                                           rsGenAppForm.setValue("grid_height", "90");
                                                           rsGenAppForm.showItem("rc_storage_type");
                                                           rsGenAppForm.hideItem("ui_facets");
                                                           rsGenAppWindow.show();
                                                       }},
                                                   {title: "RedisJSON", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isJsonEnabled(), click: function() {
                                                           const windowAppContext = (window as any)._appContext_;
                                                           rsGenAppForm.clearErrors(false);
                                                           rsGenAppForm.clearValue("app_prefix");
                                                           rsGenAppForm.setValue("app_name", "Document Application");
                                                           rsGenAppForm.setValue("app_type", "RedisJSON");
                                                           rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                                           // @ts-ignore
                                                           rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                                           rsGenAppForm.setValue("grid_height", "90");
                                                           rsGenAppForm.hideItem("rc_storage_type");
                                                           rsGenAppForm.hideItem("ui_facets");
                                                           rsGenAppWindow.show();
                                                       }},
                                                   {title: "RediSearch", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isStructureFlat() | windowAppContext.isJsonEnabled(), click: function() {
                                                           const windowAppContext = (window as any)._appContext_;
                                                           rsGenAppForm.clearErrors(false);
                                                           rsGenAppForm.clearValue("app_prefix");
                                                           rsGenAppForm.setValue("app_name", "Search Application");
                                                           rsGenAppForm.setValue("app_type", "RediSearch");
                                                           if (windowAppContext.isStructureFlat())
                                                               rsGenAppForm.setValue("ds_structure", "Flat");
                                                           else
                                                               rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                                           // @ts-ignore
                                                           rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                                           rsGenAppForm.setValue("grid_height", "80");
                                                           rsGenAppForm.hideItem("rc_storage_type");
                                                           rsGenAppForm.showItem("ui_facets");
                                                           rsGenAppWindow.show();
                                                       }},
                                                   {title: "RedisGraph", icon: "[SKIN]/actions/ai-rediscore-icon.png", enabled: windowAppContext.isGraphEnabled(), click: function() {
                                                           const windowAppContext = (window as any)._appContext_;
                                                           rsGenAppForm.clearErrors(false);
                                                           rsGenAppForm.clearValue("app_prefix");
                                                           rsGenAppForm.setValue("app_name", "Graph Application");
                                                           rsGenAppForm.setValue("app_type", "RedisGraph");
                                                           rsGenAppForm.setValue("ds_structure", "Hierarchy");
                                                           // @ts-ignore
                                                           rsGenAppForm.setValue("ds_title", windowAppContext.getAppViewTitle());
                                                           rsGenAppForm.setValue("grid_height", "80");
                                                           rsGenAppForm.hideItem("ui_facets");
                                                           rsGenAppForm.hideItem("rc_storage_type");
                                                           rsGenAppWindow.show();
                                                       }}
                                               ]}
                                       ]},
                                   {isSeparator: true},
                                   {title: "Export Data ...", icon: "[SKIN]/actions/ai-export-icon.png", enabled: true,
                                       submenu: [
                                           {title: "Grid as PDF", icon: "[SKIN]/actions/ai-export-grid-pdf-icon.png", click: function() {
                                                   const windowAppViewGrid = (window as any).appViewGrid;
                                                   let appViewGrid: isc.ListGrid;
                                                   appViewGrid = windowAppViewGrid;
                                                   // @ts-ignore
                                                   isc.Canvas.showPrintPreview(appViewGrid);
                                               }},
                                           {title: "Grid as CSV", icon: "[SKIN]/actions/ai-export-grid-csv-icon.png", click: function() {
                                                   const windowAppContext = (window as any)._appContext_;
                                                   DataModeler.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                                               }},
                                           {title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function() {
                                                   DataModeler.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                                               }},
                                           {title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function() {
                                                   DataModeler.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                                               }}
                                       ]}
                               ]
                           });
        }
        else
        {
            // ToolStrip "File" menu for disabled
            fileMenu = isc.Menu.create({
                               ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                               data: [
                                   {title: "Applications ...", icon: "[SKIN]/actions/ai-application-commands-icon.png",
                                       submenu: [
                                           {title: "Manage", icon: "[SKIN]/actions/ai-application-manage-icon.png", enabled: true, click: function() {
                                                   applicationsGrid.deselectAllRecords();
                                                   ragWindow.show();
                                               }}
                                       ]},
                                   {title: "Flat Data ...", icon: "[SKIN]/actions/ai-data-flat-command-icon.png",
                                       submenu: [
                                           {title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-data-flat-upload-icon.png", click: function() {
                                                   dafcUploadForm.clearValues();
                                                   dafxUploadForm.clearValues();
                                                   dafWindow.show();
                                               }},
                                           {title: "Manage", icon: "[SKIN]/actions/ai-data-flat-manage.png", enabled: true, click: function() {
                                                   dataFlatGrid.deselectAllRecords();
                                                   dataFlatGrid.invalidateCache();
                                                   dataFlatGrid.filterData({});
                                                   dafgWindow.show();
                                               }}
                                       ]},
                                   {title: "Hierarchy Data ...", icon: "[SKIN]/actions/ai-data-hierarchy-command-icon.png",
                                       submenu: [
                                           {title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-icon.png",
                                               submenu: [
                                                   {title: "JSON", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-json-icon.png", click: function() {
                                                           jsonfUploadForm.clearValues();
                                                           jsonxUploadForm.clearValues();
                                                           jsonWindow.show();
                                                       }},
                                                   {title: "Graph", enabled: true, icon: "[SKIN]/actions/ai-data-hierarchy-upload-graph-icon.png", click: function() {
                                                           graphnUploadForm.clearValues();
                                                           grapheUploadForm.clearValues();
                                                           graphWindow.show();
                                                       }}
                                               ]},
                                           {title: "Manage", icon: "[SKIN]/actions/ai-data-hierarchy-manage-icon.png", enabled: true, click: function() {
                                                   dataHierarchyGrid.deselectAllRecords();
                                                   dataHierarchyGrid.invalidateCache();
                                                   dataHierarchyGrid.filterData({});
                                                   dahgWindow.show();
                                               }}
                                       ]},
                                   {isSeparator: true},
                                   {title: "Document ...", icon: "[SKIN]/actions/ai-document-commands-icon.png",
                                       submenu: [
                                           {title: "Upload", enabled: true, icon: "[SKIN]/actions/ai-document-upload-icon.png", click: function() {
                                                   docUploadForm.clearValues();
                                                   dufWindow.show();
                                               }},
                                           {title: "Manage", enabled: true, icon: "[SKIN]/actions/ai-document-manage-icon.png", click: function() {
                                                   documentsGrid.deselectAllRecords();
                                                   dgWindow.show();
                                               }}
                                       ]}
                               ]
                           });
        }

        let fileMenuButton = isc.ToolStripMenuButton.create({
                              ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
                          });

        // ToolStrip "Schema" button and related grid and forms
        let tsSchemaButton: isc.ToolStripButton;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled()))
        {
            let schemaGrid: isc.ListGrid;
            if (scDataSource.getFieldNames(false).length > 18)
            {
                schemaGrid = isc.ListGrid.create({
                                     ID:"schemaGrid", width: 710, height: 500, autoDraw: false, dataSource: "DM-SchemaGrid",
                                     initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                     autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                                     alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                     listEndEditAction: "next", autoSaveEdits: false,
                                     // @ts-ignore
                                     getCellCSSText: function (record, rowNum, colNum) {
                                         if (colNum == 0)
                                             return "font-weight:bold; color:#000000;";
                                     }
                                 });
            }
            else
            {
                schemaGrid = isc.ListGrid.create({
                                     ID:"schemaGrid", width: 710, height: 300, autoDraw: false, dataSource: "DM-SchemaGrid",
                                     initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                     autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                                     alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                     listEndEditAction: "next", autoSaveEdits: false,
                                     // @ts-ignore
                                     getCellCSSText: function (record, rowNum, colNum) {
                                         if (colNum == 0)
                                             return "font-weight:bold; color:#000000;";
                                     }
                                 });
            }

            let sgApplyButton = isc.Button.create({
                                          ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false,
                                          // @ts-ignore
                                          click: function() {
                                              schemaGrid.saveAllEdits();
                                              isc.Notify.addMessage("Updates saved", null, null, {
                                                  canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                              });
                                          }
                                      });
            let sgDiscardButton = isc.Button.create({
                                            ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                schemaGrid.discardAllEdits();
                                                isc.Notify.addMessage("Updates discarded", null, null, {
                                                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                                });
                                            }
                                        });
            let sgCloseButton = isc.IButton.create({
                                           ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                           // @ts-ignore
                                           click: function() {
                                               sgWindow.hide();
                                           }
                                       });
            let sgButtonLayout = isc.HStack.create({
                                           ID: "sgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                           autoDraw: false, membersMargin: 40,
                                           members: [ sgApplyButton, sgDiscardButton, sgCloseButton ]
                                       });
            let sgFormLayout = isc.VStack.create({
                                             ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                             layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                             members:[ schemaGrid, sgButtonLayout ]
                                         });
            let sgWindow = isc.Window.create({
                                             ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                                             isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ sgFormLayout ]
                                         });
            tsSchemaButton = isc.ToolStripButton.create({
                                    ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                                    // @ts-ignore
                                    click: function()
                                    {
                                        schemaGrid.invalidateCache();
                                        schemaGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                                              function (aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest) {
                                                                  let gridData : isc.List;
                                                                  gridData = aData;
                                                                  const windowAppViewGrid = (window as any).appViewGrid;
                                                                  let appViewGrid: isc.ListGrid;
                                                                  appViewGrid = windowAppViewGrid;
                                                                  let isVisible : boolean;
                                                                  let gridRecord : isc.ListGridRecord;
                                                                  for (let recOffset = 0; recOffset < gridData.getLength(); recOffset++)
                                                                  {
                                                                      isVisible = false;
                                                                      gridRecord = gridData.get(recOffset);
                                                                      let listGridFields = appViewGrid.getFields();
                                                                      for (let lgField of listGridFields)
                                                                      {
                                                                          // @ts-ignore
                                                                          if (gridRecord.item_name == lgField.name)
                                                                          {
                                                                              isVisible = true;
                                                                              // @ts-ignore
                                                                              gridRecord.item_title = lgField.title;
                                                                          }
                                                                      }
                                                                      // @ts-ignore
                                                                      gridRecord.isVisible = isVisible;
                                                                  }
                                                              });
                                        sgWindow.show();
                                    }
                                });
        }
        else // Graph node and relationship schemas
        {
            let nodeSchemaGrid = isc.ListGrid.create({
                                 ID:"nodeSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "DM-NodeSchemaGrid",
                                 initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                 autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                                 alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                 listEndEditAction: "next", autoSaveEdits: false,
                                 // @ts-ignore
                                 getCellCSSText: function (record, rowNum, colNum) {
                                     if (colNum == 0)
                                         return "font-weight:bold; color:#000000;";
                                 }
                             });
            let relSchemaGrid = isc.ListGrid.create({
                                 ID:"relSchemaGrid", width: 710, height: 428, autoDraw: false, dataSource: "DM-RelSchemaGrid",
                                 initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                 autoFetchData: false, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
                                 alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                                 listEndEditAction: "next", autoSaveEdits: false,
                                 // @ts-ignore
                                 getCellCSSText: function (record, rowNum, colNum) {
                                     if (colNum == 0)
                                         return "font-weight:bold; color:#000000;";
                                 }
                             });
            let tsGraphSchemaTab = isc.TabSet.create({
                               ID: "tsGraphSchemaTab", tabBarPosition: "top", width: 725, height: 475, autoDraw: false,
                               tabs: [
                                       {title: "Node Schema", pane: nodeSchemaGrid },
                                       {title: "Relationship Schema", pane: relSchemaGrid }
                                     ]
                           });

            let sgApplyButton = isc.Button.create({
                                          ID: "sgApplyButton", title: "Update", autoFit: true, autoDraw: false,
                                          // @ts-ignore
                                          click: function() {
                                              const windowGraphSchemaTab = (window as any).tsGraphSchemaTab;
                                              let tsGraphSchemaTab: isc.TabSet;
                                              tsGraphSchemaTab = windowGraphSchemaTab;
                                              let selectedTabOffset = tsGraphSchemaTab.getSelectedTabNumber();
                                              if (selectedTabOffset == 0)
                                              {
                                                  nodeSchemaGrid.saveAllEdits();
                                                  isc.Notify.addMessage("Node updates saved", null, null, {
                                                      canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                                  });
                                              }
                                              else
                                              {
                                                  relSchemaGrid.saveAllEdits();
                                                  isc.Notify.addMessage("Relationship updates saved", null, null, {
                                                      canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                                  });
                                              }

                                          }
                                      });
            let sgDiscardButton = isc.Button.create({
                                            ID: "sgDiscardButton", title: "Discard", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                nodeSchemaGrid.discardAllEdits();
                                                relSchemaGrid.discardAllEdits();
                                                isc.Notify.addMessage("Updates discarded", null, null, {
                                                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                                                });
                                            }
                                        });
            let sgCloseButton = isc.IButton.create({
                                           ID: "sgCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                           // @ts-ignore
                                           click: function() {
                                               sgWindow.hide();
                                           }
                                       });
            let sgButtonLayout = isc.HStack.create({
                                           ID: "sgButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                           autoDraw: false, membersMargin: 40,
                                           members: [ sgApplyButton, sgDiscardButton, sgCloseButton ]
                                       });
            let sgFormLayout = isc.VStack.create({
                                             ID: "sgFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                             layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                             members:[ tsGraphSchemaTab, sgButtonLayout ]
                                         });
            let sgWindow = isc.Window.create({
                                             ID: "sgWindow", title: "Schema Editor Window", autoSize: true, autoCenter: true,
                                             isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ sgFormLayout ]
                                         });
            tsSchemaButton = isc.ToolStripButton.create({
                                        ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                                        // @ts-ignore
                                        click: function()
                                        {
                                            nodeSchemaGrid.invalidateCache();
                                            nodeSchemaGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                                                  function (aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest) {
                                                                      let gridData : isc.List;
                                                                      gridData = aData;
                                                                      const windowAppViewGrid = (window as any).appViewGrid;
                                                                      let appViewGrid: isc.ListGrid;
                                                                      appViewGrid = windowAppViewGrid;
                                                                      let isVisible : boolean;
                                                                      let gridRecord : isc.ListGridRecord;
                                                                      for (let recOffset = 0; recOffset < gridData.getLength(); recOffset++)
                                                                      {
                                                                          isVisible = false;
                                                                          gridRecord = gridData.get(recOffset);
                                                                          let listGridFields = appViewGrid.getFields();
                                                                          for (let lgField of listGridFields)
                                                                          {
                                                                              // @ts-ignore
                                                                              if (gridRecord.item_name == lgField.name)
                                                                              {
                                                                                  isVisible = true;
                                                                                  // @ts-ignore
                                                                                  gridRecord.item_title = lgField.title;
                                                                              }
                                                                          }
                                                                          // @ts-ignore
                                                                          gridRecord.isVisible = isVisible;
                                                                      }
                                                                      relSchemaGrid.invalidateCache();
                                                                      relSchemaGrid.filterData(DataModeler.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                                                  });
                                            sgWindow.show();
                                        }
                                    });
        }

        // ToolStrip "Add, Edit and Delete" buttons and related forms
        let avfWindow: isc.Window;
        let appViewForm: isc.DynamicForm;
        let appViewFormLayout: isc.VStack;
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled()))
        {
            if (scDataSource.getFieldNames(false).length > 50)
            {
                appViewForm = isc.DynamicForm.create({
                                     ID: "appViewForm", width: 600, height: 500, numCols:8, autoDraw: false,
                                     dataSource: windowAppContext.getAppViewDS()
                                 });
            }
            else if (scDataSource.getFieldNames(false).length > 30)
            {
                appViewForm = isc.DynamicForm.create({
                                     ID: "appViewForm", width: 600, height: 400, numCols:6, autoDraw: false,
                                     dataSource: windowAppContext.getAppViewDS()
                                 });
            }
            else if (scDataSource.getFieldNames(false).length > 18)
            {
                appViewForm = isc.DynamicForm.create({
                                     ID: "appViewForm", width: 600, height: 300, numCols:4, autoDraw: false,
                                     dataSource: windowAppContext.getAppViewDS()
                                 });
            }
            else
            {
                appViewForm = isc.DynamicForm.create({
                                     ID: "appViewForm", width: 300, height: 300, numCols:2, autoDraw: false,
                                     dataSource: windowAppContext.getAppViewDS()
                                 });
            }
            let avfSaveButton = isc.Button.create({
                                      ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          if (appViewForm.valuesAreValid(false, false))
                                          {
                                              windowAppContext.assignFormContext(appViewForm);
                                              // @ts-ignore
                                              appViewForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
                                              avfWindow.hide();
                                          }
                                          else
                                              appViewForm.validate(false);
                                      }
                                  });
            let avfCancelButton = isc.IButton.create({
                                     ID: "avfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                     // @ts-ignore
                                     click: function() {
                                         avfWindow.hide();
                                     }
                                 });
            let avfButtonLayout = isc.HStack.create({
                                    ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                    members: [ avfSaveButton, avfCancelButton ]
                                });
            appViewFormLayout = isc.VStack.create({
                                  ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                  layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                  members:[ appViewForm, avfButtonLayout ]
                              });
            avfWindow = isc.Window.create({
                                  ID: "avfWindow", title: "Data Record Window", autoSize: true, canDragResize: true,
                                  autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                  items: [ appViewFormLayout ]
                              });
        }
        else
        {
            appViewForm = isc.DynamicForm.create({
                                     ID: "appViewForm", autoDraw: false, dataSource: windowAppContext.getAppViewDS()
                                 });
            let avfSaveButton = isc.Button.create({
                                      ID: "avfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          if (DataModeler.prototype.graphNodeFormsIsValid(false))
                                          {
                                              DataModeler.prototype.graphNodesToAppViewForm();
                                              windowAppContext.assignFormContext(appViewForm);
                                              // @ts-ignore
                                              appViewForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
                                              avfWindow.hide();
                                          }
                                          else
                                              DataModeler.prototype.graphNodeFormsIsValid(true);
                                      }
                                  });
            let avfCancelButton = isc.IButton.create({
                                     ID: "avfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                     // @ts-ignore
                                     click: function() {
                                         avfWindow.hide();
                                     }
                                 });
            let avfButtonLayout = isc.HStack.create({
                                    ID: "avfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 175, membersMargin: 40,
                                    members: [ avfSaveButton, avfCancelButton ]
                                });

            // Graph node form
            let graphNodeFormLayout = this.createGraphNodeFormLayout(scDataSource);
            appViewFormLayout = isc.VStack.create({
                                      ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 10,
                                      layoutBottomMargin: 10, layoutLeftMargin: 45, layoutRightMargin: 10, membersMargin: 20,
                                      members:[ graphNodeFormLayout, avfButtonLayout ]
                                  });

            // Graph relationship form
            let graphEdgeFormLayout = this.createGraphRelationshipFormLayout();
            let grAddButton = isc.ToolStripButton.create({
                                     ID: "grAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                                     // @ts-ignore
                                     click: function() {
                                         const windowGraphRelForm = (window as any).graphRelForm;
                                         let graphRelForm: isc.DynamicForm;
                                         graphRelForm = windowGraphRelForm;
                                         graphRelForm.hideItem("common_edge_direction");
                                         graphRelForm.clearValues();
                                         graphRelForm.clearErrors(false);
                                         graphRelForm.editNewRecord();
                                         graphRelForm.getItem("common_type").enable();
                                         const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                         let graphEdgeVertexForm: isc.DynamicForm;
                                         graphEdgeVertexForm = windowGraphEdgeVertexForm;
                                         graphEdgeVertexForm.clearValues();
                                         graphEdgeVertexForm.getItem("_suggest").enable();
                                         grfWindow.setTitle("Edit Relationship Form Window");
                                         grfWindow.show();
                                     }
                                 });
            let grEditButton = isc.ToolStripButton.create({
                                      ID: "grEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
                                      // @ts-ignore
                                      click: function() {
                                          const windowGraphGridRel = (window as any).graphGridRel;
                                          let graphGridRel: isc.ListGrid;
                                          graphGridRel = windowGraphGridRel;
                                          if (graphGridRel != null)
                                          {
                                              let lgRecord = graphGridRel.getSelectedRecord();
                                              if (lgRecord != null)
                                              {
                                                  const windowGraphRelForm = (window as any).graphRelForm;
                                                  let graphRelForm: isc.DynamicForm;
                                                  graphRelForm = windowGraphRelForm;
                                                  graphRelForm.hideItem("common_edge_direction");
                                                  graphRelForm.clearValues();
                                                  graphRelForm.clearErrors(false);
                                                  graphRelForm.editSelectedData(graphGridRel);
                                                  graphRelForm.getItem("common_type").disable();
                                                  grfWindow.setTitle("Edit Relationship Form Window");
                                                  const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                                  let graphEdgeVertexForm: isc.DynamicForm;
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
            let grDeleteButton = isc.ToolStripButton.create({
                                    ID: "grDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
                                    // @ts-ignore
                                    click: function() {
                                        const windowGraphGridRel = (window as any).graphGridRel;
                                        let graphGridRel: isc.ListGrid;
                                        graphGridRel = windowGraphGridRel;
                                        let lgRecord = graphGridRel.getSelectedRecord();
                                        if (lgRecord != null)
                                        {
                                            // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                            isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedGraphGridRelRow(value ? 'OK' : 'Cancel')");
                                        }
                                        else
                                            isc.say("You must select a row on the grid to remove.");
                                    }
                                });
            let grfSaveButton = isc.Button.create({
                                  ID: "grfSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      const windowGraphRelForm = (window as any).graphRelForm;
                                      let graphRelForm: isc.DynamicForm;
                                      graphRelForm = windowGraphRelForm;
                                      if (graphRelForm.valuesAreValid(false, false))
                                      {
                                          const windowAppViewForm = (window as any).appViewForm;
                                          let appViewForm: isc.DynamicForm;
                                          appViewForm = windowAppViewForm;
                                          graphRelForm.setValue("common_vertex_src_id", appViewForm.getValue("common_id"));
                                          const windowGraphEdgeVertexForm = (window as any).graphEdgeVertexForm;
                                          let graphEdgeVertexForm: isc.DynamicForm;
                                          graphEdgeVertexForm = windowGraphEdgeVertexForm;
                                          graphRelForm.setValue("common_vertex_name", graphEdgeVertexForm.getValue("_suggest"));
                                          windowAppContext.assignFormContext(graphRelForm);
                                          // @ts-ignore
                                          graphRelForm.saveData("DataModeler.prototype.updateCallback(dsResponse,data,dsRequest)");
                                          grfWindow.hide();
                                      }
                                      else
                                          graphRelForm.validate(false);
                                  }
                              });
            let grfCancelButton = isc.IButton.create({
                                 ID: "grfCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     grfWindow.hide();
                                 }
                             });
            let grfButtonLayout = isc.HStack.create({
                                ID: "grfButtonLayout", width: "100%", height: 24, layoutAlign: "center", autoDraw: false, layoutLeftMargin: 30, membersMargin: 40,
                                members: [ grfSaveButton, grfCancelButton ]
                            });
            let graphRelFormLayout = isc.VStack.create({
                                  ID: "graphRelFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                  layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                  members:[ graphEdgeFormLayout, grfButtonLayout ]
                              });
            let grfWindow = isc.Window.create({
                                  ID: "grfWindow", title: "Graph Relationship Form Window", autoSize: true, canDragResize: true,
                                  autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                  items: [ graphRelFormLayout ]
                              });
            let graphToolStrip = isc.ToolStrip.create({
                                 // @ts-ignore
                                 ID: "graphToolStrip", width: "100%", height: 32, autoDraw: false,
                                 // @ts-ignore
                                 members: [grAddButton, "separator", grEditButton, "separator", grDeleteButton]
                             });
            let graphGridRelOut = isc.ListGrid.create({
                                ID: "graphGridRel", dataSource: windowAppContext.getAppViewRelOutDS(),
                                autoDraw: false, width: 588, height: 473, autoFetchData: false, showFilterEditor: false,
                                allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false,
                                canEditTitles: false, canEdit: false, leaveScrollbarGap: false
                            });
            let tsGridLayout = isc.VStack.create({
                                  ID: "tsGridLayout", width: "100%", autoDraw: false, layoutTopMargin: 10,
                                  members:[ graphToolStrip, graphGridRelOut ]
                              });
            let tsGraphTab = isc.TabSet.create({
                                ID: "tsGraphTab", tabBarPosition: "top", width: 600, height: 575, autoDraw: false,
                                tabs: [
                                    {title: "Node", pane: appViewFormLayout },
                                    {title: "Relationships", pane: tsGridLayout,
                                        // @ts-ignore
                                        tabSelected : function (tabSet, tabNum, tabPane, ID, tab)
                                                        {
                                                            if (tabNum == 1)
                                                            {
                                                                let primaryKeyField = scDataSource.getPrimaryKeyField();
                                                                let keyName = primaryKeyField.name;
                                                                let simpleCriteria = {};
                                                                // @ts-ignore
                                                                simpleCriteria[keyName] = appViewForm.getValue(keyName);
                                                                graphGridRelOut.filterData(simpleCriteria);
                                                            }
                                                        }}
                                ]
                            });
            avfWindow = isc.Window.create({
                              ID: "avfWindow", title: "Data Record Window", width: 609, height: 625,
                              canDragResize: true, autoCenter: true, isModal: true, showModalMask: true,
                              autoDraw: false,
                              items: [ tsGraphTab ]
                          });
        }

        let tsAddButton = isc.ToolStripButton.create({
                               ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                               // @ts-ignore
                               click: function() {
                                   const windowAppContext = (window as any)._appContext_;
                                   const windowAppViewGrid = (window as any).appViewGrid;
                                   let appViewGrid: isc.ListGrid;
                                   appViewGrid = windowAppViewGrid;
                                   let lgRecord = appViewGrid.getSelectedRecord();
                                   if (windowAppContext.isGraphEnabled())
                                   {
                                       avfWindow.setTitle("Add New Record Window");
                                       appViewForm.clearValues();
                                       appViewForm.editNewRecord();
                                       const windowTSGraphTab = (window as any).tsGraphTab;
                                       let tsGraphTab: isc.TabSet;
                                       tsGraphTab = windowTSGraphTab;
                                       tsGraphTab.disableTab(1);
                                       DataModeler.prototype.graphNodeFormsClearValues();
                                   }
                                   else
                                   {
                                       if (lgRecord == null)
                                       {
                                           avfWindow.setTitle("Add New Record Window");
                                           appViewForm.clearValues();
                                           appViewForm.editNewRecord();
                                       }
                                       else
                                       {
                                           let primaryFieldName = "";
                                           let primaryDSField = scDataSource.getPrimaryKeyField();
                                           if (primaryDSField != null)
                                               primaryFieldName = primaryDSField.name;
                                           let keyValueMap = new Map();
                                           for (const [k, v] of Object.entries(lgRecord))
                                           {
                                               if (k == primaryFieldName)
                                                   keyValueMap.set(k, "");
                                               else
                                                   keyValueMap.set(k, v);
                                           }
                                           let defaultValues = {};
                                           keyValueMap.forEach((value, key) => {
                                               // @ts-ignore
                                               defaultValues[key] = value
                                           });
                                           appViewForm.editNewRecord(defaultValues);
                                           avfWindow.setTitle("Add (Duplicate) Record Window");
                                       }
                                   }
                                   appViewForm.clearErrors(false);
                                   avfWindow.show();
                               }
                           });
        let tsEditButton = isc.ToolStripButton.create({
                                 ID: "tsEditButton", icon: "[SKIN]/actions/ai-edit-icon.png", prompt: "Edit Row", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowAppViewGrid = (window as any).appViewGrid;
                                     let appViewGrid: isc.ListGrid;
                                     appViewGrid = windowAppViewGrid;
                                     if (appViewGrid != null)
                                     {
                                         let lgRecord = appViewGrid.getSelectedRecord();
                                         if (lgRecord != null)
                                         {
                                             appViewForm.clearValues();
                                             appViewForm.clearErrors(false);
                                             appViewForm.editSelectedData(appViewGrid);
                                             avfWindow.setTitle("Edit Data Form Window");
                                             if (windowAppContext.isGraphEnabled())
                                             {
                                                 const windowTSGraphTab = (window as any).tsGraphTab;
                                                 let tsGraphTab: isc.TabSet;
                                                 tsGraphTab = windowTSGraphTab;
                                                 DataModeler.prototype.appViewToGraphNodeForms();
                                                 tsGraphTab.enableTab(1);
                                                 tsGraphTab.selectTab(0);
                                                 const windowGraphRelForm = (window as any).graphRelForm;
                                                 let graphRelForm: isc.DynamicForm;
                                                 // @ts-ignore
                                                 DataModeler.prototype.showGraphNodeForm(lgRecord.common_vertex_label);
                                             }
                                             avfWindow.show();
                                         }
                                         else
                                             isc.say("You must select a row on the grid to edit.");
                                     }
                                 }
                             });
        let tsDeleteButton = isc.ToolStripButton.create({
                                  ID: "tsDeleteButton", icon: "[SKIN]/actions/remove.png", prompt: "Delete Row", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {
                                      const windowAppViewGrid = (window as any).appViewGrid;
                                      let appViewGrid: isc.ListGrid;
                                      appViewGrid = windowAppViewGrid;
                                      if (appViewGrid != null)
                                      {
                                          let lgRecord = appViewGrid.getSelectedRecord();
                                          if (lgRecord != null)
                                          {
                                              // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                              isc.confirm("Proceed with row deletion operation?", "DataModeler.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                                          }
                                          else
                                              isc.say("You must select a row on the grid to remove.");
                                      }
                                  }
                              });
        // ToDo: ToolStrip "View Document" button for RediSearch
        let tsViewButton = isc.ToolStripButton.create({
                                  ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {isc.say('Document viewing is not enabled for this configuration.');}
                              });

        // ToolStrip "Details" button and related forms
        let detailViewer = isc.DetailViewer.create({
                                                ID: "detailViewer", width: 400, height: 400, autoDraw: false, dataSource: windowAppContext.getAppViewDS(), showDetailFields: true
                                            });
        let detailLayout = isc.VStack.create({
                                                 ID: "detailLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
                                                 layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
                                                 members:[ detailViewer ]
                                             });
        let detailWindow = isc.Window.create({
                                             ID: "detailWindow", title: "Detail Window", width: 465, height: 550, autoCenter: true,
                                             isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
                                             items: [ detailLayout ]
                                         });
        let tsDetailsButton = isc.ToolStripButton.create({
                                  ID: "tsDetailsButton", icon: "[SKIN]/actions/ai-details-icon.png", prompt: "Row Details", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowAppViewGrid = (window as any).appViewGrid;
                                     let appViewGrid: isc.ListGrid;
                                     appViewGrid = windowAppViewGrid;
                                     let lgRecord = appViewGrid.getSelectedRecord();
                                     if (lgRecord != null)
                                     {
                                        detailViewer.viewSelectedData(appViewGrid);
                                        detailWindow.show();
                                     }
                                     else
                                         isc.say("You must select a row on the grid to detail.");
                                 }
                              });

        // ToolStrip "Analyze" button and related grids
        let analyzeGrid = isc.ListGrid.create({
                                ID:"analyzeGrid", width: 700, height: 400, autoDraw: false, dataSource: "DM-AnalyzeGrid",
                                autoFetchData: true, canRemoveRecords: false, canSort: false, alternateRecordStyles:true,
                                alternateFieldStyles: false, baseStyle: "alternateGridCell", showHeaderContextMenu: false,
                                autoSaveEdits: false, canEdit: false, wrapCells: true, cellHeight: 36,
                                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                  // @ts-ignore
                                  getCellCSSText: function (record, rowNum, colNum) {
                                      if (colNum == 0)
                                          return "font-weight:bold; color:#000000;";
                                  }
                            });

        let agCloseButton = isc.IButton.create({
                                                     ID: "agCloseButton", title: "Close", autoFit: true, autoDraw: false,
                                                     // @ts-ignore
                                                     click: function() {
                                                         agWindow.hide();
                                                     }
                                                 });
        let agButtonLayout = isc.HStack.create({
                                         ID: "agButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                                         autoDraw: false, membersMargin: 40,
                                         members: [ agCloseButton ]
                                     });
        let agFormLayout: isc.VStack;
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled()))
        {
            agFormLayout = isc.VStack.create({
                                     ID: "agFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                     layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                     members:[ analyzeGrid, agButtonLayout ]
                                 });
        }
        else
        {
            let analyzeRelGrid = isc.ListGrid.create({
                                     ID:"analyzeRelGrid", width: 700, height: 400, autoDraw: false, dataSource: "DM-AnalyzeRelGrid",
                                     autoFetchData: true, canRemoveRecords: false, canSort: false, alternateRecordStyles:true,
                                     alternateFieldStyles: false, baseStyle: "alternateGridCell", showHeaderContextMenu: false,
                                     autoSaveEdits: false, canEdit: false, wrapCells: true, cellHeight: 36,
                                     initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                     // @ts-ignore
                                     getCellCSSText: function (record, rowNum, colNum) {
                                         if (colNum == 0)
                                             return "font-weight:bold; color:#000000;";
                                     }
                                 });
            let agGraphAnalyzeTab = isc.TabSet.create({
                                      ID: "agGraphAnalyzeTab", tabBarPosition: "top", width: 720, height: 445, autoDraw: false,
                                      tabs: [
                                          {title: "Node Grid", pane: analyzeGrid },
                                          {title: "Relationship Grid", pane: analyzeRelGrid }
                                      ]
                                  });
            agFormLayout = isc.VStack.create({
                                 ID: "agFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                 members:[ agGraphAnalyzeTab, agButtonLayout ]
                             });
        }
        let agWindow = isc.Window.create({
                                               ID: "agWindow", title: "Data Analyzer Window", autoSize: true,
                                               autoCenter: true, isModal: false, showModalMask: false, autoDraw: false,
                                               items: [ agFormLayout ]
                                           });

        let tsAnalyzeButton = isc.ToolStripButton.create({
                                 ID: "tsAnalyzeButton", icon: "[SKIN]/actions/ai-analyze-icon.png", prompt: "Analyze Rows", autoDraw: false, showDown: false,
                                 // @ts-ignore
                                 click: function() {
                                     const windowAnalyzeGrid = (window as any).analyzeGrid;
                                     let analyzeGrid: isc.ListGrid;
                                     analyzeGrid = windowAnalyzeGrid;
                                     analyzeGrid.invalidateCache();
                                     agWindow.show();
                                 }
                             });

        // ToDo: ToolStrip "Chart" button for data visualization
        let tsChartButton = isc.ToolStripButton.create({
                                   ID: "tsChartButton", icon: "[SKIN]/actions/ai-chart-icon.png", prompt: "Show Chart", autoDraw: false, showDown: false,
                                   // @ts-ignore
                                   click: function() {
                                       isc.say("Data visualization is not enabled for this configuration.");
                                   }
                               });

        // ToolStrip "Graph Visualization" button and related forms
        let gvHTMLPane = isc.HTMLPane.create({
                                 width:800, height:600, showEdges:false, autoDraw: false,
                                 contentsURL: windowAppContext.getGraphVisualizationURL(),
                                 contentsType:"page"
                             });
        let gvOptionsForm = isc.DynamicForm.create({
                                ID: "gvOptionsForm", autoDraw: false, width: 500, colWidths: [190, "*"],
                                fields: [
                                    {name: "is_matched", title:"Is Matched", type:"text", defaultValue: "false", hidden: true},
                                    {name: "is_hierarchical", title:"Is Hierarchical", type:"text", defaultValue: "false", hidden: true},
                                    {name: "node_shape", title:"Node Shape", type:"text", editorType: "ComboBoxItem", defaultValue: "ellipse", canEdit: true, required: true, hint: "Available node shapes", wrapHintText: false, valueMap : {"ellipse" : "Ellipse", "circle" : "Circle", "box" : "Box", "square" : "Square", "database" : "Database", "text" : "Text", "diamond" : "Diamond", "star" : "Star", "triangle" : "Triangle", "triangleDown" : "Triangle Down", "hexagon" : "Hexagon"}},
                                    // @ts-ignore
                                    {name: "node_color", title:"Node Color", type:"color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false},
                                    {name: "edge_arrow", title:"Edge Arrow", type:"text", editorType: "ComboBoxItem", defaultValue: "to", canEdit: true, required: true, hint: "Available edge arrow types", wrapHintText: false, valueMap : {"to" : "To", "from" : "From", "to;from" : "To and From", "middle" : "Middle"}},
                                    // @ts-ignore
                                    {name: "edge_color", title:"Edge Color", type:"color", defaultValue: "#97C2FC", canEdit: true, required: true, hint: "Available node colors", wrapHintText: false},
                                    // @ts-ignore
                                    {name: "match_color", title:"Match Color", type:"color", defaultValue: "#FAA0A0", canEdit: true, required: true, hint: "Available match colors", wrapHintText: false}
                                ]
                            });
        let gvofApplyButton = isc.IButton.create({
                                  ID: "gvofApplyButton", title: "Apply", autoFit: true, autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      gvofWindow.hide();
                                      gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
                                  }
                              });
        let gvofDefaultsButton = isc.IButton.create({
                                 ID: "gvofDefaultsButton", title: "Defaults", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvOptionsForm.clearValue("node_shape");
                                     gvOptionsForm.clearValue("node_color");
                                     gvOptionsForm.clearValue("edge_arrow");
                                     gvOptionsForm.clearValue("edge_color");
                                     gvOptionsForm.clearValue("match_color");
                                 }
                             });
        let gvofCancelButton = isc.IButton.create({
                                      ID: "gvofCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                      // @ts-ignore
                                      click: function() {
                                          gvofWindow.hide();
                                      }
                                  });
        let gvofButtonLayout = isc.HStack.create({
                                     ID: "gvofButtonLayout", width: "100%", height: 24,
                                     layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                     members: [ gvofApplyButton, gvofDefaultsButton, gvofCancelButton ]
                                 });
        let gvoFormLayout = isc.VStack.create({
                                   ID: "gvoFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                   layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                   members:[ gvOptionsForm, gvofButtonLayout ]
                               });
        let gvofWindow = isc.Window.create({
                                   ID: "gvofWindow", title: "Graph Options Window", autoSize: true, autoCenter: true,
                                   isModal: true, showModalMask: true, autoDraw: false,
                                   items: [ gvoFormLayout ]
                               });

        let gvOptionsButton = isc.IButton.create({
                                 ID: "gvOptionsButton", title: "Options", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvofWindow.show();
                                 }
                             });
        let gvRefreshButton = isc.IButton.create({
                               ID: "gvRefreshButton", title: "Refresh", autoFit: true, autoDraw: false,
                               // @ts-ignore
                               click: function() {
                                   gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
                               }
                           });
        let gvDownloadButton = isc.IButton.create({
                                 ID: "gvDownloadButton", title: "Download", autoFit: true, autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(true));
                                 }
                             });
        let gvCloseButton = isc.IButton.create({
                               ID: "gvCloseButton", title: "Close", autoFit: true, autoDraw: false,
                               // @ts-ignore
                               click: function() {
                                   gvWindow.hide();
                               }
                           });

        let gvButtonLayout = isc.HStack.create({
                               ID: "gvButtonLayout", width: "100%", height: 24, layoutAlign: "center",
                               autoDraw: false, membersMargin: 40, redrawOnResize: true,
                               members: [ gvOptionsButton, gvRefreshButton, gvDownloadButton, gvCloseButton ]
                           });
        let gvFormLayout = isc.VStack.create({
                                 ID: "gvFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                 layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                 members:[ gvHTMLPane, gvButtonLayout ]
                             });

        let gvWindow = isc.Window.create({
                             ID: "gvWindow", title: "Graph Visualization Window", autoSize: true, canDragResize: true,
                             autoCenter: true, isModal: false, showModalMask: false, autoDraw: false, redrawOnResize: true,
                             items: [ gvFormLayout ]
                         });

        let tsGraphButton = isc.ToolStripButton.create({
                                  ID: "tsGraphButton", icon: "[SKIN]/actions/ai-graph-icon.png", prompt: "Show Graph", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {
                                      gvOptionsForm.setValue("is_matched", "false");
                                      gvOptionsForm.setValue("is_hierarchical", "false");
                                      gvWindow.setTitle("Graph Visualization Window");
                                      gvWindow.show();
                                      gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
                                  }
                              });
        let tsGraphMatchedButton = isc.ToolStripButton.create({
                               ID: "tsGraphMatchedButton", icon: "[SKIN]/actions/ai-graph-match-icon.png", prompt: "Show Matched Graph", autoDraw: false, showDown: false,
                               // @ts-ignore
                               click: function() {
                                   gvOptionsForm.setValue("is_matched", "true");
                                   gvOptionsForm.setValue("is_hierarchical", "false");
                                   gvWindow.setTitle("Graph Visualization Window (Matched)");
                                   gvWindow.show();
                                   gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
                               }
                           });
        let tsGraphTreeButton = isc.ToolStripButton.create({
                              ID: "tsGraphTreeButton", icon: "[SKIN]/actions/ai-graph-tree-icon.png", prompt: "Show Graph As Tree", autoDraw: false, showDown: false,
                              // @ts-ignore
                              click: function() {
                                  gvOptionsForm.setValue("is_hierarchical", "true");
                                  gvWindow.setTitle("Graph Visualization Window (Tree)");
                                  gvWindow.show();
                                  gvHTMLPane.setContentsURL(DataModeler.prototype.graphVisualizationOptionsToURL(false));
                              }
                          });

        // ToDo: ToolStrip "Map" button
        let tsMapButton = isc.ToolStripButton.create({
                                   ID: "tsMapButton", icon: "[SKIN]/actions/ai-maps-icon.png", prompt: "Show Map", autoDraw: false,
                                   // @ts-ignore
                                   click: function() {isc.say('Map viewing is not enabled for this configuration.');}
                               });

        // ToolStrip "Application Grid" button
        let tsApplicationGridButton = isc.ToolStripButton.create({
                                              ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
                                              // @ts-ignore
                                              click: function() {
                                                  const windowAppViewGrid = (window as any).appViewGrid;
                                                  const windowReleaseGrid = (window as any).releaseGrid;
                                                  const windowAppLayout = (window as any).appLayout;
                                                  let releaseGrid: isc.ListGrid;
                                                  releaseGrid = windowReleaseGrid;
                                                  let appViewGrid: isc.ListGrid;
                                                  appViewGrid = windowAppViewGrid;
                                                  let appLayout: isc.VStack;
                                                  appLayout = windowAppLayout;
                                                  appLayout.showMember(appViewGrid);
                                                  appLayout.hideMember(releaseGrid);
                                              }
                                          });

        // ToolStrip "Release Grid" button
        let tsReleaseGridButton = isc.ToolStripButton.create({
                                                 ID: "tsReleaseGridButton", icon: "[SKIN]/actions/ai-release-number-grid-icon.png", prompt: "Release Grid", autoDraw: false,
                                                 // @ts-ignore
                                                 click: function() {
                                                     const windowAppViewGrid = (window as any).appViewGrid;
                                                     const windowReleaseGrid = (window as any).releaseGrid;
                                                     const windowAppLayout = (window as any).appLayout;
                                                     let releaseGrid: isc.ListGrid;
                                                     releaseGrid = windowReleaseGrid;
                                                     let appViewGrid: isc.ListGrid;
                                                     appViewGrid = windowAppViewGrid;
                                                     let appLayout: isc.VStack;
                                                     appLayout = windowAppLayout;
                                                     appLayout.hideMember(appViewGrid);
                                                     appLayout.showMember(releaseGrid);
                                                     let filterMap = new Map();
                                                     filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                                                     filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                                                     filterMap.set("_appPrefix", windowAppContext.getPrefix());
                                                     filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                                                     filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                                                     filterMap.set("_limit", windowAppContext.getCriteriaLimit());
                                                     filterMap.set("_action", "reload");
                                                     let simpleCriteria = {};
                                                     filterMap.forEach((value, key) => {
                                                         // @ts-ignore
                                                         simpleCriteria[key] = value
                                                     });
                                                     releaseGrid.invalidateCache();
                                                     releaseGrid.filterData(simpleCriteria);
                                                 }
                                             });

        // ToolStrip "RedisInsight" button and web page launcher
        let tsRedisInsightButton = isc.ToolStripButton.create({
                                                          ID: "tsRedisInsightButton", icon: "[SKIN]/actions/ai-redis-insight-icon.png", prompt: "Redis Insight", autoDraw: false,
                                                          // @ts-ignore
                                                          click: function() {
                                                              window.open(windowAppContext.getRedisInsightURL(), "_blank");
                                                          }
                                                      });

        // ToolStrip "Settings" button and related forms
        let setGeneralForm = isc.DynamicForm.create({
                                  ID: "setGeneralForm", autoDraw: false, width: 500, colWidths: [190, "*"],
                                  fields: [
                                        {name: "app_group", title:"App Group", type:"text", value: windowAppContext.getGroupName(), canEdit: true, required: true, hint: "Application group", wrapHintText: false},
                                        {name: "app_name", title:"App Name", type:"text", value: windowAppContext.getAppName(), canEdit: true, required: true, hint: "Application name", wrapHintText: false},
                                        {name: "ds_name", title:"DS Name", type:"text", value: windowAppContext.getAppViewDS(), canEdit: false, hint: "Data source name", wrapHintText: false},
                                        {name: "ds_title", title:"DS Title", type:"text",  value: windowAppContext.getAppViewTitle(), canEdit: false, required: true, hint: "Data source title", wrapHintText: false}
                                      ]
                            });
        let setGridForm = isc.DynamicForm.create({
                                    ID: "setGridForm", autoDraw: false, width: 500, colWidths: [190, "*"], isGroup: true, groupTitle: "Grid Options",
                                    fields: [
                                        // @ts-ignore
                                        {name: "fetch_policy", title: "Fetch Policy", type: "SelectItem", hint: "Page navigation", canEdit: false, wrapHintText: false, defaultValue: "Virtual Paging", valueMap : {"virtual" : "Virtual Paging", "paging" : "Single Paging"}},
                                        // @ts-ignore
                                        {name: "page_size", title: "Page Size", editorType: "SpinnerItem", writeStackedIcons: true, canEdit: false, hint: "Rows per page", wrapHintText: false, defaultValue: 50, min: 19, max: 100, step: 10},
                                        // @ts-ignore
                                        {name: "column_filtering", title: "Column Filtering", type: "radioGroup", defaultValue: "Disabled", valueMap: ["Disabled", "Enabled"], vertical:false},
                                        // @ts-ignore
                                        {name: "csv_header", title: "CSV Header", type: "radioGroup", defaultValue: "Title", valueMap: ["Title", "Field/Type/Title"], vertical:false}
                                    ]
                                });
        let settingsSaveButton = isc.Button.create({
                                         ID: "settingsSaveButton", title: "Save", autoFit: true, autoDraw: false,
                                         // @ts-ignore
                                         click: function() {
                                             if (setGeneralForm.valuesAreValid(false, false))
                                             {
                                                 const windowAppContext = (window as any)._appContext_;
                                                 windowAppContext.setGroupName(setGeneralForm.getValue("app_group"));
                                                 windowAppContext.setAppName(setGeneralForm.getValue("app_name"));
                                                 const windowHeaderHTMLFlow = (window as any).headerHTMLFlow;
                                                 let headerHTMLFlow: isc.HTMLFlow;
                                                 headerHTMLFlow = windowHeaderHTMLFlow;
                                                 headerHTMLFlow.setContents(DataModeler.prototype.createHTMLHeader());
                                                 headerHTMLFlow.redraw();
                                                 const windowAppViewGrid = (window as any).appViewGrid;
                                                 let appViewGrid: isc.ListGrid;
                                                 appViewGrid = windowAppViewGrid;
                                                 if (setGridForm.getValue("column_filtering") == "Enabled")
                                                     appViewGrid.setShowFilterEditor(true);
                                                 else
                                                     appViewGrid.setShowFilterEditor(false);
                                                 if (setGridForm.getValue("csv_header") == "Title")
                                                     windowAppContext.setGridCSVHeader("title");
                                                 else
                                                     windowAppContext.setGridCSVHeader("field");
                                                 // @ts-ignore
                                                 settingsWindow.hide();
                                             }
                                             else
                                                 setGeneralForm.validate(false);
                                         }
                                     });
        let settingsCancelButton = isc.IButton.create({
                                            ID: "settingsCancelButton", title: "Cancel", autoFit: true, autoDraw: false,
                                            // @ts-ignore
                                            click: function() {
                                                settingsWindow.hide();
                                            }
                                        });
        let settingsButtonLayout = isc.HStack.create({
                                           ID: "settingsButtonLayout", width: "100%", height: 24,
                                           layoutAlign: "center", autoDraw: false, membersMargin: 40,
                                                 members: [ settingsSaveButton, settingsCancelButton ]
                                       });
        let settingsFormLayout = isc.VStack.create({
                                             ID: "settingsFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                                   layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                             members:[ setGeneralForm, setGridForm, settingsButtonLayout ]
                                         });
        let settingsWindow = isc.Window.create({
                                         ID: "settingsWindow", title: "Settings Window", autoSize: true, autoCenter: true,
                                         isModal: true, showModalMask: true, autoDraw: false,
                                         items: [ settingsFormLayout ]
                                     });
        let tsSettingsButton = isc.ToolStripButton.create({
                                  ID: "tsSettingsButton", icon: "[SKIN]/actions/ai-settings-gear-black-icon.png", prompt: "Settings", autoDraw: false,
                                  // @ts-ignore
                                  click: function() {
                                      setGeneralForm.clearErrors(false);
                                      settingsWindow.show();
                                  }
                              });

        // ToolStrip "Help" button and web page launcher
        let tsHelpButton = isc.ToolStripButton.create({
                                 ID: "tsHelpButton", icon: "[SKIN]/actions/ai-help-icon.png", prompt: "Online Help", autoDraw: false,
                                 // @ts-ignore
                                 click: function() {
                                     let offset = window.location.href.lastIndexOf("/") + 1;
                                     let urlBase = window.location.href.substring(0, offset);
                                     let urlHelpDoc = urlBase + "doc/UG-RedisAppStudio.pdf";
                                     window.open(urlHelpDoc, "_blank");
                                 }
                             });

        // ToolStrip creation and button assignment
        let commandToolStrip : isc.ToolStrip;
        if (windowAppContext.isModelerEnabled())
        {
            if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled()))
            {
                commandToolStrip = isc.ToolStrip.create({
                                        // @ts-ignore
                                        ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                                        // @ts-ignore
                                        members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsAnalyzeButton, "separator", "starSpacer", tsSettingsButton, tsHelpButton]
                                    });
            }
            else
            {
                commandToolStrip = isc.ToolStrip.create({
                                        // @ts-ignore
                                        ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                                        // @ts-ignore
                                        members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, tsAnalyzeButton, "separator", tsGraphButton, tsGraphMatchedButton, tsGraphTreeButton, "starSpacer", tsSettingsButton, tsHelpButton]
                                    });
            }
        }
        else
        {
            commandToolStrip = isc.ToolStrip.create({
                                // @ts-ignore
                                ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                                // @ts-ignore
                                members: [fileMenuButton, "separator", "starSpacer", tsApplicationGridButton, tsReleaseGridButton, tsHelpButton]
                            });
        }

        return commandToolStrip;
    }

    // Create the main data grid
    private createAppViewGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        // @ts-ignore
        let CustomListGrid = isc.defineClass("CustomListGrid", "ListGrid").addProperties({
                init: function () {
                            this.Super("init", arguments);
                            var toolStrip = isc.ToolStrip.create({
                                 membersMargin: 5, autoDraw: false,
                                 members: [
                                     isc.Label.create({
                                              ID: "gridPosition",
                                              wrap: false, padding: 5, autoDraw: false,
                                              contents: "0 to 0 of 0",
                                              // @ts-ignore
                                              getRowRangeText: function(arrayVisibleRows, totalRows, lengthIsKnown) {
                                                  if (! lengthIsKnown)
                                                      return "Loading...";
                                                  else if (arrayVisibleRows[0] != -1)
                                                  {
                                                      let adjTotalRows = totalRows - 1;
                                                      return isc.NumberUtil.format((arrayVisibleRows[0] + 1), "#,##0") + " to " + isc.NumberUtil.format((arrayVisibleRows[1]), "#,##0") + " of " + isc.NumberUtil.format(adjTotalRows, "#,##0");
                                                  }
                                                  else
                                                      return "0 to 0 of 0";
                                              }
                                          }),
                                     isc.LayoutSpacer.create({width: "*"}),
                                     // @ts-ignore
                                     "separator",
                                     isc.ImgButton.create({
                                                  grid: this, src: "[SKIN]/actions/refresh.png", showRollOver: false,
                                                  prompt: "Refresh", width: 16, height: 16, showDown: false, autoDraw: false,
                                                  // @ts-ignore
                                                  click: function () {
                                                      const windowAppContext = (window as any)._appContext_;
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

        let appViewGrid : isc.ListGrid;
        if ((windowAppContext.isStructureFlat()) || (windowAppContext.isJsonEnabled()))
        {
            // @ts-ignore
            appViewGrid = CustomListGrid.create({
                                ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(), autoDraw: false,
                                width: "100%", height: windowAppContext.getGridHeightPercentage(),
                                autoFetchData: windowAppContext.isModelerEnabled(), showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: true,
                                useAdvancedFieldPicker: true, canEditTitles: true, expansionFieldImageShowSelected: false,
                                canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, canGroupBy: true, groupByMaxRecords:1200,
                                initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                                recordDoubleClick: function () {
                                    const windowAppViewGrid = (window as any).appViewGrid;
                                    const windowDetailViewer = (window as any).detailViewer;
                                    const windowDetailWindow = (window as any).detailWindow;
                                    let appViewGrid: isc.ListGrid;
                                    appViewGrid = windowAppViewGrid;
                                    let detailViewer: isc.DetailViewer;
                                    detailViewer = windowDetailViewer;
                                    let detailWindow: isc.Window;
                                    detailWindow = windowDetailWindow;
                                    let lgRecord = appViewGrid.getSelectedRecord();
                                    if (lgRecord != null)
                                    {
                                        detailViewer.viewSelectedData(appViewGrid);
                                        detailWindow.show();
                                    }
                                    else
                                        isc.say("You must select a row on the grid to detail.");
                                }
                            });
        }
        else
        {
            // @ts-ignore
            appViewGrid = CustomListGrid.create({
                            ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(),
                            autoDraw: false, width: "100%", height: windowAppContext.getGridHeightPercentage(),
                            autoFetchData: windowAppContext.isModelerEnabled(), showFilterEditor: false,
                            allowFilterOperators: false, filterOnKeypress: true, useAdvancedFieldPicker: true,
                            canEditTitles: true, expansionFieldImageShowSelected: false, canExpandRecords: true,
                            expansionMode: "related", detailDS: windowAppContext.getAppViewRelDS(), canEdit: false, leaveScrollbarGap: false,
                            initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                            recordDoubleClick: function () {
                                const windowAppViewGrid = (window as any).appViewGrid;
                                const windowDetailViewer = (window as any).detailViewer;
                                const windowDetailWindow = (window as any).detailWindow;
                                let appViewGrid: isc.ListGrid;
                                appViewGrid = windowAppViewGrid;
                                let detailViewer: isc.DetailViewer;
                                detailViewer = windowDetailViewer;
                                let detailWindow: isc.Window;
                                detailWindow = windowDetailWindow;
                                let lgRecord = appViewGrid.getSelectedRecord();
                                if (lgRecord != null)
                                {
                                    detailViewer.viewSelectedData(appViewGrid);
                                    detailWindow.show();
                                }
                                else
                                    isc.say("You must select a row on the grid to detail.");
                            }
                        });
        }

        return appViewGrid;
    }

    // Create the Release History Grid
    private createReleaseGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        let releaseGrid = isc.ListGrid.create({
                              ID: "releaseGrid", dataSource: "DM-ReleaseGrid", autoDraw: false, width: "100%",
                              height: windowAppContext.getGridHeightPercentage(), autoFetchData: false, showFilterEditor: false,
                              allowFilterOperators: false, filterOnKeypress: false, useAdvancedFieldPicker: false, canEditTitles: false,
                              expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false, leaveScrollbarGap: false,
                              alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                              wrapCells: true, cellHeight: 50,
                              // @ts-ignore
                              getCellCSSText: function (record, rowNum, colNum) {
                                  if (this.getFieldName(colNum) == "release_number")
                                      return "font-weight:bold; color:#000000;";
                              }
                          });

        return releaseGrid;
    }

    // Initializes all UI controls for the application
    init(): void
    {
        // The following allows you to assign default font and UI control sizes
        // Invoke for Dense density: isc.Canvas.resizeFonts(1); isc.Canvas.resizeControls(2);
        // Invoke for Compact density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(4);
        // Invoke for Medium density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(6);
        // Invoke for Spacious density: isc.Canvas.resizeFonts(3); isc.Canvas.resizeControls(10);
        isc.Canvas.resizeFonts(1);
        isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", {multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200});

        // Create our UI components
        // Do not enable this without adding a timer to prevent concurrent fetches while your SessionContext is being created.
        // let appPropGrid = isc.ListGrid.create({ID: "appPropGrid", dataSource: "RW-AppPropGrid", autoFetchData: true, visibility: "hidden"});
        const windowAppContext = (window as any)._appContext_;

        let headerSection = this.createHeaderSection();
        let searchSection = this.createSearchSection();
        let commandToolStrip = this.createCommandToolStrip();
        let appViewGrid = this.createAppViewGrid();
        let releaseGrid = this.createReleaseGrid();

        if (windowAppContext.isModelerEnabled())
            this.appLayout = isc.VStack.create({
                                                   ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                                                   members: [headerSection, searchSection, commandToolStrip, appViewGrid]
                                               });
        else
        {
            this.appLayout = isc.VStack.create({
                               ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                               members:[ headerSection, commandToolStrip, appViewGrid, releaseGrid ]
                           });
            this.appLayout.hideMember(releaseGrid);
        }
    }

    // Shows the application
    show(): void
    {
        this.appLayout.show();
    }

    // Hides the application
    hide(): void
    {
        this.appLayout.hide();
    }
}