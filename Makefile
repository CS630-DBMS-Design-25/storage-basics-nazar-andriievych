CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pedantic
TARGET = storage_cli
SRCS = main.cpp storage_layer.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean 