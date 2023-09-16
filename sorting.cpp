#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>
#include <sys/resource.h>
#include <sys/time.h>

void merge(std::vector<int>& arr, int start, int mid, int end);
void mergeSort(std::vector<int>& arr, int start, int end);
void parallelMergeSort(std::vector<int>& arr, int start, int end, int maxDepth);

long getMemoryUsage() {
    struct rusage rusage;
    getrusage(RUSAGE_SELF, &rusage);
    return rusage.ru_maxrss;  // this value is in kilobytes (KB) on most platforms
}

bool isAlmostSorted(const std::vector<int>& arr, double fraction = 0.01) {
    int outOfPlaceCount = 0;
    for (size_t i = 1; i < arr.size(); i++) {
        if (arr[i] < arr[i - 1]) {
            outOfPlaceCount++;
        }
    }
    return (static_cast<double>(outOfPlaceCount) / arr.size()) < fraction;
}

int THRESHOLD;

int main() {
    // Read N from the input file
    std::ifstream inputFile("input_test_2.csv");
    if (!inputFile.is_open()) {
        std::cerr << "Error: Unable to open input file.\n";
        return 1;
    }

    std::string line;
    std::getline(inputFile, line);  // Read the first line to get N from input.csv
    int N;
    if (line.find("# ") != std::string::npos) {
        N = std::stoi(line.substr(2));  // Extract N from the line
    } else {
        std::cerr << "Error: Invalid input file format.\n";
        return 1;
    }

    // Get the maximum number of processing cores from the input file
    int max_cores = N;


    // Calculate the thread count based on N and available CPU cores
    int numThreads = std::min(max_cores, static_cast<int>(std::thread::hardware_concurrency()));

    // Read the input numbers into a vector
    std::vector<int> numbers;
    int num;
    while (inputFile >> num) {
        numbers.push_back(num);
    }

    // Display the number of CPU cores
    std::cout << "Number of CPU cores: " << numThreads << "\n";
    


    int baseThreshold = numbers.size() / numThreads;


    // small dataset

    if (numbers.size() < 500) {
      THRESHOLD = baseThreshold * 4;
    }

// Large Dataset with Few CPU cores
else if (numbers.size() >= 10000 && numThreads <= 2) {
    THRESHOLD = baseThreshold * 1.5;  
}

// Large Dataset with Many CPU cores
else if (numbers.size() > 10000 && numThreads > 2) {
    THRESHOLD = baseThreshold;
  
}

// Almost Sorted Small Dataset
else if (numbers.size() <= 1000 && isAlmostSorted(numbers)) {
    THRESHOLD = baseThreshold * 4;
}

else {
     THRESHOLD = baseThreshold * 4;
}



    std::cerr << "Threshold value " << THRESHOLD << "\n";

    std::cerr << "total numbers " << numbers.size() << "\n";

    std::cout << "Is Almost Sorted: " << isAlmostSorted(numbers) << "\n";

    // Measure the starting memory usage
    long beforeMemory = getMemoryUsage();

    // Measure the starting time
    auto start_time = std::chrono::high_resolution_clock::now();

    // Perform parallelized merge sort
    parallelMergeSort(numbers, 0, numbers.size() - 1, numThreads);

    // Measure the ending time
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the elapsed time
    std::chrono::duration<double> elapsed_time = end_time - start_time;

    // Measure the ending memory usage
    long afterMemory = getMemoryUsage();
    
    // Calculate the memory used during sorting
    long memoryUsed = afterMemory - beforeMemory;

    std::cout << "Elapsed time: " << elapsed_time.count() << " seconds" << std::endl;
    std::cout << "Memory used during sorting: " << memoryUsed << " KB" << std::endl;

    // Write the sorted numbers and CPU core count to the output file
    std::ofstream outputFile("output.csv");
    outputFile << "CPU Cores Used: " << numThreads << "\n";
    for (const auto& num : numbers) {
        outputFile << num << "\n";
    }

    return 0;
}


void merge(std::vector<int>& arr, int start, int mid, int end) {
    int leftSize = mid - start + 1;
    int rightSize = end - mid;

    std::vector<int> leftArr(leftSize);
    std::vector<int> rightArr(rightSize);

    // Copy the left and right halves to temporary arrays
    for (int i = 0; i < leftSize; ++i) {
        leftArr[i] = arr[start + i];
    }

    for (int j = 0; j < rightSize; ++j) {
        rightArr[j] = arr[mid + 1 + j];
    }

    int i = 0, j = 0, k = start;

    // Merge the left and right halves in-place
    while (i < leftSize && j < rightSize) {
        if (leftArr[i] <= rightArr[j]) {
            arr[k++] = leftArr[i++];
        } else {
            arr[k++] = rightArr[j++];
        }
    }

    // Copy remaining elements from the left and right arrays (if any)
    while (i < leftSize) {
        arr[k++] = leftArr[i++];
    }

    while (j < rightSize) {
        arr[k++] = rightArr[j++];
    }
}



void mergeSort(std::vector<int>& arr, int start, int end) {
    if (start < end) {
        int mid = start + (end - start) / 2;

        mergeSort(arr, start, mid);
        mergeSort(arr, mid + 1, end);

        merge(arr, start, mid, end);
    }
}


void parallelMergeSort(std::vector<int>& arr, int start, int end, int depth) {
    
    if (start < end) {
        if (depth <= 0 || (end - start) < THRESHOLD) {  // THRESHOLD can be set to a suitable value, e.g., 1000
            mergeSort(arr, start, end);
            return;
        }

        int mid = start + (end - start) / 2;

        std::thread leftThread(parallelMergeSort, std::ref(arr), start, mid, depth - 1);
        parallelMergeSort(arr, mid + 1, end, depth - 1);  // Do one half in the current thread
        leftThread.join();

        merge(arr, start, mid, end);
    }
}
