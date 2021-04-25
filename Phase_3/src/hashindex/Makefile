# modify the paths appropriately
JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test hash

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core




