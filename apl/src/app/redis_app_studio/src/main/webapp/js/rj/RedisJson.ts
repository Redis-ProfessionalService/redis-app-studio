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
 * The Redis App Studio Data Modeler is responsible for managing
 * an in-memory grid for a user who is interested in modeling their data
 * set prior to storing it in Redis. The RedisCore class defines and
 * manages the complete SmartClient application. While this logic could
 * be broken out to other smaller files, the decision was made to keep it
 * self-contained to simplify application generation and minimize browser
 * load times.
 */
class RedisJson
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

        return "<table class=\"ahTable\"> <col width=\"1%\"> <col width=\"5%\"> <col width=\"15%\">" +
            " <col width=\"58%\"> <col width=\"20%\"> <col width=\"1%\"> <tr> <td>&nbsp;</td>" +
            "  <td><img alt=\"Redis App Studio\" class=\"ahImage\" src=\"images/redis-app-studio.svg\" height=\"99\" width=\"60\"></td>" +
            "  <td class=\"ahGroup\">" + windowAppContext.getGroupName() + "</td> <td>&nbsp;</td>" +
            "  <td class=\"ahName\">" + windowAppContext.getAppName() + "</td>\n" +
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
            _limit: fetchLimit,
            _redisStorageType: windowAppContext.getRedisStorageType()
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
        let filterMap = new Map();
        filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
        filterMap.set("_dsStructure", windowAppContext.getDSStructure());
        filterMap.set("_appPrefix", windowAppContext.getPrefix());
        filterMap.set("_action", anAction);
        filterMap.set("_format", aFormat);
        filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
        filterMap.set("_offset", windowAppContext.getCriteriaOffset());
        filterMap.set("_limit", aFetchLimit);                                        // need to ensure page is filled with rows
        filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());  // row management
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

    // Handles main grid fetch operations
    private executeAppViewGridFetch(): void
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
        filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());  // row management
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
            }
        }
    }

    // Schema: reload the full document data set
    private rebuildDatabase(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowSchemaGrid = (window as any).schemaGrid;
            let schemaGrid: isc.ListGrid;
            schemaGrid = windowSchemaGrid;
            if (schemaGrid != null)
            {
                // We use the remove operation to trigger the index rebuild operation on the server side
                schemaGrid.removeData(schemaGrid.data.get(0));
                isc.Notify.addMessage("Database rebuild initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                // Allow time for the index rebuild and notification to complete before schema refresh
                // setTimeout(() => { schemaGrid.invalidateCache(); schemaGrid.filterData(RedisJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle())); }, 2000);
            }
        }
    }

    // Database: flush all data from database
    private flushDatabase(aResponse: string): void
    {
        if (aResponse == "OK")
        {
            const windowAppContext = (window as any)._appContext_;
            const windowRedisDBInfoWindow = (window as any).redisDBInfoWindow;
            let redisDBInfoWindow: isc.Window;
            redisDBInfoWindow = windowRedisDBInfoWindow;
            const windowRedisDBInfoForm = (window as any).redisDBInfoForm;
            let redisDBInfoForm: isc.DynamicForm;
            redisDBInfoForm = windowRedisDBInfoForm;
            if ((redisDBInfoWindow != null) && (redisDBInfoForm != null))
            {
                // We use the remove operation to trigger the database flush operation on the server side
                windowAppContext.assignFormContext(redisDBInfoForm);
                redisDBInfoForm.saveData();
                redisDBInfoWindow.hide();
                isc.Notify.addMessage("Database flush initiated", null, null, {
                    canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
                });
                // Allow time for the index rebuild and notification to complete before schema refresh
                setTimeout(() => { RedisJson.prototype.executeAppViewGridFetch(); }, 2000);
            }
        }
    }

    // Main grid: form save callback that generates a notification message.
    private updateCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        isc.Notify.addMessage("Form saved", null, null, {
            canDismiss: true, appearMethod: "slide", disappearMethod: "fade", position: "T"
        });
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
        dafxUploadForm.saveData("RedisJson.prototype.uploadCallback(dsResponse,data,dsRequest)");
    }

    // This function is called after the first data hierarchy file has been uploaded.
    private graphNextUploadCallback(aDSResponse: isc.DSResponse, aData: any, aDSRequest: isc.DSRequest): void
    {
        const windowDataHierarchyNodeForm = (window as any).graphnUploadForm;
        let graphnUploadForm: isc.DynamicForm;
        graphnUploadForm = windowDataHierarchyNodeForm;
        const windowDataHierarchyEdgeForm = (window as any).grapheUploadForm;
        let grapheUploadForm: isc.DynamicForm;
        grapheUploadForm = windowDataHierarchyEdgeForm;
        grapheUploadForm.setValue("document_title", graphnUploadForm.getValue("document_title"));
        grapheUploadForm.setValue("document_description", graphnUploadForm.getValue("document_description"));
        // @ts-ignore
        grapheUploadForm.saveData("RedisJson.prototype.uploadCallback(dsResponse,data,dsRequest)");
    }

    // Create the ToolStrip component with command buttons
    private createCommandToolStrip(): isc.ToolStrip
    {
        const windowAppContext = (window as any)._appContext_;

        // ToolStrip "File" Menu Item - Redis Data->Information form
        let redisDBInfoForm = isc.DynamicForm.create({
                                                 ID: "redisDBInfoForm", width: 400, height: 400, autoDraw: false, dataSource: "RJ-Database", autoFetchData:false, canEdit: false
                                             });
        let redisDBInfoLayout = isc.VStack.create({
                                                      ID: "redisDBInfoLayout", width: "100%", height: "100%", autoDraw: false, layoutAlign: "center",
                                                      layoutTopMargin: 20, layoutBottomMargin: 20, layoutLeftMargin: 20, layoutRightMargin: 20,
                                                      members:[ redisDBInfoForm ]
                                                  });
        let redisDBInfoWindow = isc.Window.create({
                                                      ID: "redisDBInfoWindow", title: "Redis DB Info Window", width: 410, height: 520, autoCenter: true,
                                                      isModal: false, showModalMask: false, canDragResize: true, autoDraw: false,
                                                      items: [ redisDBInfoLayout ]
                                                  });

        // ToolStrip "File" menu
        let fileMenu = isc.Menu.create({
                         ID: "fileMenu", showShadow: true, shadowDepth: 10, autoDraw: false,
                         data: [
                             {title: "Redis Data ...", icon: "[SKIN]/actions/ai-save-icon.png",
                                 submenu: [
                                     {title: "Information", icon: "[SKIN]/actions/ai-redis-connect-icon.png", enabled: true, checked: false, click: function() {
                                             const windowAppContext = (window as any)._appContext_;
                                             windowAppContext.assignFormContext(redisDBInfoForm);
                                             redisDBInfoForm.fetchData(RedisJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                                 redisDBInfoWindow.show();
                                             });
                                         }},
                                     {title: "Flush DB", icon: "[SKIN]/actions/ai-commands-delete-icon.png", enabled: true, click: function() {
                                             const windowAppContext = (window as any)._appContext_;
                                             windowAppContext.assignFormContext(redisDBInfoForm);
                                             redisDBInfoForm.fetchData(RedisJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()), function () {
                                                 redisDBInfoWindow.show();
                                                 // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                 isc.confirm("Are you sure you want to flush all data?", "RedisJson.prototype.flushDatabase(value ? 'OK' : 'Cancel')");
                                             });
                                         }}
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
                                             RedisJson.prototype.executeAppViewGridExport("grid_export_by_criteria_csv", windowAppContext.getGridCSVHeader(), 100);
                                         }},
                                     {title: "Grid as JSON", icon: "[SKIN]/actions/ai-export-grid-json-icon.png", click: function() {
                                             RedisJson.prototype.executeAppViewGridExport("grid_export_by_criteria_json", "json", 100);
                                         }},
                                     {title: "Schema as XML", icon: "[SKIN]/actions/ai-export-schema-xml-icon.png", click: function() {
                                             RedisJson.prototype.executeAppViewGridExport("schema_export_xml", "xml", 100);
                                         }},
                                     {title: "Commands as TXT", icon: "[SKIN]/actions/ai-commands-icon.png", click: function() {
                                             RedisJson.prototype.executeAppViewGridExport("command_export_txt", "txt", 100);
                                         }}
                                ]}
                         ]
                     });
        let fileMenuButton = isc.ToolStripMenuButton.create({
                              ID: "fileMenuButton", title: "File", autoDraw: false, menu: fileMenu
                          });

        // ToolStrip "Schema" button and related grid and forms
        let schemaGrid: isc.ListGrid;
        let scDataSource = isc.DataSource.get(windowAppContext.getAppViewDS());
        if (scDataSource.getFieldNames(false).length > 18)
        {
            schemaGrid = isc.ListGrid.create({
                             ID:"schemaGrid", width: 710, height: 500, autoDraw: false, dataSource: "RJ-SchemaGrid",
                             initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                             autoFetchData: true, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
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
                             ID:"schemaGrid", width: 710, height: 300, autoDraw: false, dataSource: "RJ-SchemaGrid",
                             initialCriteria: this.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()),
                             autoFetchData: true, canEdit: true, canSort: false, showHeaderContextMenu: false, editEvent: "click",
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
        let sgRebuildButton = isc.Button.create({
                                                    ID: "sgRebuildButton", title: "Rebuild", autoFit: true, autoDraw: false,
                                                    // @ts-ignore
                                                    click: function() {
                                                        // @ts-ignore  - You needed to look at the compiled JavaScript to get the callback name correct
                                                        isc.confirm("Rebuilding the database will destroy existing data - proceed with operation?", "RedisJson.prototype.rebuildDatabase(value ? 'OK' : 'Cancel')");
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
                                                   members: [ sgApplyButton, sgRebuildButton, sgDiscardButton, sgCloseButton ]
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
        let tsSchemaButton = isc.ToolStripButton.create({
                                            ID: "tsSchemaButton", icon: "[SKIN]/actions/ai-schema-icon.png", prompt: "Schema Form", showDown: false, autoDraw: false,
                                            // @ts-ignore
                                            click: function()
                                            {
                                                schemaGrid.invalidateCache();
                                                schemaGrid.filterData(RedisJson.prototype.defaultCriteria(windowAppContext.getDSStructure(), windowAppContext.getAppViewTitle()));
                                                sgWindow.show();
                                            }
                                        });

        // ToolStrip "Add, Edit and Delete" buttons and related forms
        let appViewForm: isc.DynamicForm;
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
                                      appViewForm.saveData("RedisJson.prototype.updateCallback(dsResponse,data,dsRequest)");
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
        let appViewFormLayout = isc.VStack.create({
                                ID: "appViewFormLayout", width: "100%", align: "center", autoDraw: false, layoutTopMargin: 30,
                                layoutBottomMargin: 30, layoutLeftMargin: 30, layoutRightMargin: 30, membersMargin: 20,
                                members:[ appViewForm, avfButtonLayout ]
                           });
        let avfWindow = isc.Window.create({
                                             ID: "avfWindow", title: "Redis Form Window", autoSize: true, canDragResize: true,
                                             autoCenter: true, isModal: true, showModalMask: true, autoDraw: false,
                                             items: [ appViewFormLayout ]
                                         });
        let tsAddButton = isc.ToolStripButton.create({
                               ID: "tsAddButton", icon: "[SKIN]/actions/add.png", prompt: "Add Row", autoDraw: false, showDown: false,
                               // @ts-ignore
                               click: function()
                               {
                                   const windowAppViewGrid = (window as any).appViewGrid;
                                   let appViewGrid: isc.ListGrid;
                                   appViewGrid = windowAppViewGrid;
                                   let lgRecord = appViewGrid.getSelectedRecord();
                                   if (lgRecord == null)
                                   {
                                       avfWindow.setTitle("Add Redis Form Window");
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
                                       avfWindow.setTitle("Add (Duplicate) Redis Form Window");
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
                                             avfWindow.setTitle("Edit Redis Form Window");
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
                                              isc.confirm("Proceed with row deletion operation?", "RedisJson.prototype.deleteSelectedAppViewGridRow(value ? 'OK' : 'Cancel')");
                                          }
                                          else
                                              isc.say("You must select a row on the grid to remove.");
                                      }
                                  }
                              });

        // ToDo: ToolStrip "Document View" button
        let tsViewButton = isc.ToolStripButton.create({
                                  ID: "tsViewButton", icon: "[SKIN]/actions/ai-commands-view-icon.png", prompt: "View Document", autoDraw: false, showDown: false,
                                  // @ts-ignore
                                  click: function() {isc.say('View document selected');}
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

        // ToolStrip "Application Grid" button
        let tsApplicationGridButton = isc.ToolStripButton.create({
                                         ID: "tsApplicationGridButton", icon: "[SKIN]/actions/ai-application-gird-icon.png", prompt: "Application Grid", autoDraw: false,
                                         // @ts-ignore
                                         click: function() {
                                             const windowAppViewGrid = (window as any).appViewGrid;
                                             const windowCommandGrid = (window as any).commandGrid;
                                             const windowAppLayout = (window as any).appLayout;
                                             let commandGrid: isc.ListGrid;
                                             commandGrid = windowCommandGrid;
                                             let appViewGrid: isc.ListGrid;
                                             appViewGrid = windowAppViewGrid;
                                             let appLayout: isc.VStack;
                                             appLayout = windowAppLayout;
                                             appLayout.showMember(appViewGrid);
                                             appLayout.hideMember(commandGrid);
                                         }
                                     });

        // ToolStrip "Command Grid" button
        let tsCommandGridButton = isc.ToolStripButton.create({
                                     ID: "tsCommandGridButton", icon: "[SKIN]/actions/ai-command-list-icon.png", prompt: "Command Grid", autoDraw: false,
                                     // @ts-ignore
                                     click: function() {
                                         const windowAppViewGrid = (window as any).appViewGrid;
                                         const windowCommandGrid = (window as any).commandGrid;
                                         const windowAppLayout = (window as any).appLayout;
                                         let commandGrid: isc.ListGrid;
                                         commandGrid = windowCommandGrid;
                                         let appViewGrid: isc.ListGrid;
                                         appViewGrid = windowAppViewGrid;
                                         let appLayout: isc.VStack;
                                         appLayout = windowAppLayout;
                                         appLayout.hideMember(appViewGrid);
                                         appLayout.showMember(commandGrid);
                                         let filterMap = new Map();
                                         filterMap.set("_dsTitle", windowAppContext.getAppViewTitle());
                                         filterMap.set("_dsStructure", windowAppContext.getDSStructure());
                                         filterMap.set("_appPrefix", windowAppContext.getPrefix());
                                         filterMap.set("_fetchPolicy", windowAppContext.getFetchPolicy());
                                         filterMap.set("_offset", windowAppContext.getCriteriaOffset());
                                         filterMap.set("_limit", windowAppContext.getCriteriaLimit());
                                         filterMap.set("_redisStorageType", windowAppContext.getRedisStorageType());  // row management
                                         filterMap.set("_action", "reload");
                                         let simpleCriteria = {};
                                         filterMap.forEach((value, key) => {
                                             // @ts-ignore
                                             simpleCriteria[key] = value
                                         });
                                         commandGrid.invalidateCache();
                                         commandGrid.filterData(simpleCriteria);
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
                                                         headerHTMLFlow.setContents(RedisJson.prototype.createHTMLHeader());
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
                                                         let curHighlightColor = windowAppContext.getHighlightFontColor();
                                                         let newHighlightColor = setGridForm.getValue("highlight_color");
                                                         if (curHighlightColor != newHighlightColor)
                                                         {
                                                             windowAppContext.setHighlightFontColor(newHighlightColor);
                                                             windowAppContext.setHighlightsAssigned(false);
                                                         }
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
        let commandToolStrip = isc.ToolStrip.create({
                             // @ts-ignore
                             ID: "commandToolStrip", width: "100%", height: "3%", autoDraw: false,
                             // @ts-ignore
                             members: [fileMenuButton, "separator", tsSchemaButton, "separator", tsAddButton, tsEditButton, tsDeleteButton, tsDetailsButton, "separator", "starSpacer", tsApplicationGridButton, tsCommandGridButton, tsRedisInsightButton, tsSettingsButton, tsHelpButton]
                         });

        return commandToolStrip;
    }

    // Create the Application View Grid
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
                                                                                       RedisJson.prototype.executeAppViewGridFetch();
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

        // @ts-ignore
        let appViewGrid = CustomListGrid.create({
                            ID: "appViewGrid", dataSource: windowAppContext.getAppViewDS(), autoDraw: false, width: "100%",
                            height: windowAppContext.getGridHeightPercentage(), autoFetchData: true, showFilterEditor: false,
                            allowFilterOperators: false, filterOnKeypress: true, useAdvancedFieldPicker: true,  leaveScrollbarGap: false,
                            canEditTitles: true, expansionFieldImageShowSelected: false, canExpandRecords: false, canEdit: false,
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

        return appViewGrid;
    }

// Create the Application View Grid
    private createCommandGrid(): isc.ListGrid
    {
        const windowAppContext = (window as any)._appContext_;

        let commandGrid = isc.ListGrid.create({
                            ID: "commandGrid", dataSource: "RJ-DocCmdGrid", autoDraw: false, width: "100%",
                            height: windowAppContext.getGridHeightPercentage(), autoFetchData: false,
                            showFilterEditor: false, allowFilterOperators: false, filterOnKeypress: false,
                            useAdvancedFieldPicker: false, canEditTitles: false, expansionFieldImageShowSelected: false,
                            canExpandRecords: false, canEdit: false, leaveScrollbarGap: false, wrapCells: true, cellHeight: 50,
                            alternateRecordStyles:true, alternateFieldStyles: false, baseStyle: "alternateGridCell",
                            // @ts-ignore
                            recordDoubleClick: function () {
                                const windowCommandGrid = (window as any).commandGrid;
                                let commandGrid: isc.ListGrid;
                                commandGrid = windowCommandGrid;
                                let lgRecord = commandGrid.getSelectedRecord();
                                if (lgRecord != null)
                                {
                                    // @ts-ignore
                                    window.open(lgRecord.command_link, "_blank");
                                }
                                else
                                    isc.say("You must select a row on the grid to show help details.");
                            },
                            // @ts-ignore
                            getCellCSSText: function (record, rowNum, colNum) {
                              if (this.getFieldName(colNum) == "redis_command")
                                  return "font-weight:bold; color:#000000;";
                              else if (this.getFieldName(colNum) == "redis_parameters")
                                  return "font-weight:lighter; font-style: italic; color:#000000;";
                            }
                        });

        return commandGrid;
    }

    // Initializes all UI controls for the application
    init(): void
    {
        // The following allows you to assign default font and UI control sizes
        // Invoke for Dense density: isc.Canvas.resizeFonts(1); isc.Canvas.resizeControls(2);
        // Invoke for Compact density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(4);
        // Invoke for Medium density: isc.Canvas.resizeFonts(2); isc.Canvas.resizeControls(6);
        // Invoke for Spacious density: isc.Canvas.resizeFonts(3); isc.Canvas.resizeControls(10);
        isc.Canvas.resizeFonts(1); isc.Canvas.resizeControls(2);
        isc.Notify.configureMessages("message", {multiMessageMode: "replace", autoFitMaxWidth: 250, slideSpeed: 200});

        // Create our UI components
        let headerSection = this.createHeaderSection();
        let commandToolStrip = this.createCommandToolStrip();
        let appViewGrid = this.createAppViewGrid();
        let commandGrid = this.createCommandGrid();
        this.appLayout = isc.VStack.create({
                            ID: "appLayout", width: "100%", height: "100%", autoDraw: false, layoutTopMargin: 5, membersMargin: 0,
                            members:[ headerSection, commandToolStrip, appViewGrid, commandGrid ]
                          });
        this.appLayout.hideMember(commandGrid);
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