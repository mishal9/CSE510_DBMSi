JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

AppDriver:AppDriverPhase3.java
	$(JAVAC) AppDriverPhase3.java

driver: AppDriverPhase3
	$(JAVA) driver.AppDriverPhase3