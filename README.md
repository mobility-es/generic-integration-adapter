Generic Integration Adapter
===========================

An AppearIQ Integration Adapter which stores all documents and attachments sent by data sync from devices, and distributes them to all connected devices.


Prerequisites
-------------

* Java JDK 7 or 8 [Download](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

* Maven 3.x [Download](http://maven.apache.org/download.cgi)

 	
Installation
------------

1. Clone this Git repository.

2. Edit the file `src/main/aiq.properties` and set your orgname, username and password.

3. Run the command:
```
mvn aiq:ia.deploy
```