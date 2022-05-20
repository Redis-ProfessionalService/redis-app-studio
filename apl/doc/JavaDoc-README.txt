
JavaDoc Creation Tasks
======================

1) Locate the JavaDoc JAR file for the package you want to include or update.

   /Users/acole/GitHub/redis-app-studio/maven/.m2/repository/com/redis/ds/ds_graph/1.0/ds_graph-1.0-javadoc.jar

2) Prepare the "$APL/doc" for the new package.

   $ cd $APL/doc
   $ mkdir ds_graph

3) Extract the documentation files.

   $ cd $APL/src/doc/ds_graph
   $ jar -xf /Users/acole/GitHub/redis-app-studio/maven/.m2/repository/com/redis/ds/ds_graph/1.0/ds_graph-1.0-javadoc.jar

3) Clean up the HTML files with better document titles.

   $ cd $APL/src/doc/ds_graph
   $ grep Caerus *.html
   
   Edit each one so they refer to something like "Graph Data Source"

   $ vi allclasses-index.html allpackages-index.html help-doc.html index-all.html overview-tree.html

4) Add the new package to the parent HTML index file.

   $ cd $APL/doc
   $ vi index.html

   At or around line 70, you need to clone two "even" or "odd" row depending on what the last
   row style was (to keep the look and feel consistent).  After cloning them, then you need
   to update the "href" and description information.

   <div class="col-first even-row-color all-packages-table all-packages-table-tab1"><a href="ds_graph/index.html">Data Source Graph Classes</a></div>
   <div class="col-last even-row-color all-packages-table all-packages-table-tab1">Collection of data source graph management classes.</div>

5) Load the web page and validate the updates and new link references.

   $ open index.html
