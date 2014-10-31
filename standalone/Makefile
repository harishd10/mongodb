
# OS Name (Linux or Darwin)
OSUPPER = $(shell uname -s 2>/dev/null | tr [:lower:] [:upper:])
OSLOWER = $(shell uname -s 2>/dev/null | tr [:upper:] [:lower:])

# Flags to detect 32-bit or 64-bit OS platform
OS_SIZE = $(shell uname -m | sed -e "s/i.86/32/" -e "s/x86_64/64/")
OS_ARCH = $(shell uname -m | sed -e "s/i386/i686/")

# These flags will override any settings
ifeq ($(i386),1)
	OS_SIZE = 32
	OS_ARCH = i686
endif

ifeq ($(x86_64),1)
	OS_SIZE = 64
	OS_ARCH = x86_64
endif

# Flags to detect either a Linux system (linux) or Mac OSX (darwin)
DARWIN = $(strip $(findstring DARWIN, $(OSUPPER)))

# Location of the CUDA Toolkit binaries and libraries
CUDA_PATH       ?= /usr/local/cuda
CUDA_INC_PATH   ?= $(CUDA_PATH)/include
CUDA_BIN_PATH   ?= $(CUDA_PATH)/bin
ifneq ($(DARWIN),)
  CUDA_LIB_PATH  ?= $(CUDA_PATH)/lib
else
  ifeq ($(OS_SIZE),32)
    CUDA_LIB_PATH  ?= $(CUDA_PATH)/lib
  else
    CUDA_LIB_PATH  ?= $(CUDA_PATH)/lib64
  endif
endif

# Common binaries
NVCC            ?= $(CUDA_BIN_PATH)/nvcc
GCC             ?= g++

# Extra user flags
EXTRA_NVCCFLAGS ?=
EXTRA_LDFLAGS   ?=

# CUDA code generation flags
GENCODE_SM30    := -gencode arch=compute_30,code=sm_30 -gencode arch=compute_35,code=sm_35
GENCODE_SM35    := -gencode arch=compute_35,code=sm_35
GENCODE_FLAGS   :=  $(GENCODE_SM35) 


# OS-specific build flags
ifneq ($(DARWIN),) 
      LDFLAGS   := -L$(CUDA_LIB_PATH) -lcudart -lcudadevrt
else
      LDFLAGS   := -L$(CUDA_LIB_PATH) -lcudart -lcudadevrt
endif

# OS-architecture specific flags
ifeq ($(OS_SIZE),32)
      CCFLAGS   += -m32 -fopenmp
      NVCCFLAGS := -m32 -dc
      NVCCLFLAGS := -m32 -Xcompiler -fopenmp
else
      CCFLAGS   += -m64 -fopenmp
      NVCCFLAGS := -m64 -dc -O3 -use_fast_math
      NVCCLFLAGS := -m64 -Xcompiler -fopenmp -O3 -use_fast_math
endif

# Debug build flags
ifeq ($(dbg),1)
      CCFLAGS   += -g
      NVCCFLAGS += -g -G
      CFLAGS = -g -std=c++0x -fopenmp
      TARGET    := debug
else
	  CFLAGS = -O3 -w -std=c++0x -fopenmp
      TARGET    := release
endif


CC = g++ 
RM = /bin/rm -rf

MAKEDEPEND=${CC} -MM ${CFLAGS}

INCDIRS = -I. -I$(CUDA_INC_PATH)  -I$(CUDA_PATH)/samples/common/inc
LIBS = -lboost_thread -lboost_filesystem -lboost_program_options -lboost_system -lboost_iostreams

BIN = buildIndex queryIndex sampleData

OBJS = main.o KdIndex.o

all: buildIndex queryIndex sampleData

SOBJS = SampleData.o

sampleData: ${SOBJS}
	${CC} ${SOBJS} ${LIBS} -o $@ 

buildIndex : ${OBJS}
	${CC} ${OBJS} ${LIBS} -o $@ 

-include $(OBJS:.o=.d)

QOBJS = query.o CudaDb.o CudaDevice.o cuda_kernels.o MongoPerf.o
queryIndex : ${QOBJS}
	$(NVCC) $(NVCCLFLAGS) $(GENCODE_FLAGS) -o $@ $+ $(LDFLAGS) $(EXTRA_LDFLAGS) ${LIBS}

cuda_kernels.o : cuda_kernels.cu
	$(NVCC) $(NVCCFLAGS) $(EXTRA_NVCCFLAGS) $(GENCODE_FLAGS) ${INCDIRS} -o $@ -c $<
	
%.o : %.cpp
	${CC} ${CFLAGS} ${INCDIRS} -c $< 
	${MAKEDEPEND} $< > $*.d

.PHONY : clean 
clean :
	${RM} ${BIN}
	${RM} ${OBJS}
	${RM} ${QOBJS}
	${RM} ${SOBJS}
	${RM} *.d


