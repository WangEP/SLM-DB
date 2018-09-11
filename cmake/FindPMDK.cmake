# Find the Intel's PMEM library.
# Output variables:
#  PMEM_INCLUDE_DIR : e.g., /usr/include/.
#  PMEM_LIBRARY     : Library path of PMEM library
#  PMEM_FOUND       : True if found.

FIND_PATH(PMDK_INCLUDE_DIR NAME libpmem.h
        HINTS $ENV{HOME}/local/include /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(PMEM_LIBRARY NAME pmem
        HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
        )

FIND_LIBRARY(PMEMCTO_LIBRARY NAME pmemcto
        HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
        )

FIND_LIBRARY(PMEMOBJ_LIBRARY NAME pmemobj
        HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
        )

FIND_LIBRARY(PMEMLOG_LIBRARY NAME pmemlog
        HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
        )

FIND_LIBRARY(VMEM_LIBRARY NAME vmem
        HINTS $ENV{HOME}/local/lib64 $ENV{HOME}/local/lib /usr/local/lib64 /usr/local/lib /opt/local/lib64 /opt/local/lib /usr/lib64 /usr/lib
        )

IF (PMDK_INCLUDE_DIR AND PMEM_LIBRARY AND PMEMCTO_LIBRARY AND PMEMOBJ_LIBRARY AND PMEMLOG_LIBRARY AND VMEM_LIBRARY)
    SET(PMDK_FOUND TRUE)
    MESSAGE(STATUS "Found PMDK: inc=${PMDK_INCLUDE_DIR}")
ELSE ()
    SET(PMDK_FOUND FALSE)
    MESSAGE(STATUS "WARNING: PMDK not found.")
    MESSAGE(STATUS "Install https://github.com/pmem/pmdk")
ENDIF ()
