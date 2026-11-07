-dontwarn java.awt.**
-keep class com.sun.jna.* { *; }
-keep class com.sun.jna.ptr.* { *; }
-keepattributes RuntimeVisibleAnnotations
-keep class com.noma4i.dblayer.** extends com.sun.jna.Structure {
    public <fields>;
    public <init>(...);
}
-keep class com.noma4i.dblayer.** implements com.sun.jna.Callback {
    public <methods>;
}
-keepclassmembers class com.noma4i.dblayer.** {
    native <methods>;
}
