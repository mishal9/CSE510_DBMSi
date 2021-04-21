JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

AppDriver:AppDriver.java
	$(JAVAC) AppDriver.java

driver: AppDriver
	$(JAVA) driver.AppDriver

AppDriverPhase3:AppDriverPhase3.java
	$(JAVAC) AppDriverPhase3.java

driver3: AppDriverPhase3
	$(JAVA) driver.AppDriverPhase3