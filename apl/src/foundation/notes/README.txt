
Foundation Class Development Notes
==================================



IntelliJ Debugger Settings
--------------------------

After you upgraded to the JDK 17 release, you noticed that IntelliJ reported
the following error message whenever a debug session was invoked.

"Java HotSpot(TM) 64-Bit Server VM warning: Sharing is only supported for
boot loader classes because bootstrap classpath has been appended"

https://stackoverflow.com/questions/54205486/how-to-avoid-sharing-is-only-supported-for-boot-loader-classes-because-bootstra

To fix this, you selected "IntelliJIDEA->Preferences..." and executed a
search for "async" and led you to:
"Build, Execution, Deployment -> Debugger -> Async Stack Traces"
Deselect "Instrumenting agent (requires debugger restart)" and
that corrected the issue.

