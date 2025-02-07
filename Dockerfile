FROM alpine

COPY /build/libs/table-1.0.0-SNAPSHOT.jar table.jar
COPY /locale/ /locale/