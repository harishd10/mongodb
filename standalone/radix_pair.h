#ifndef RADIX_H
#define RADIX_H
#include <inttypes.h>

// ============================================================================
// TIMING
// ============================================================================
#ifdef _WIN32
#include <windows.h>
#define WALLCLOCK() ((double)GetTickCount()*1e-3)

#else

#include <sys/time.h>
inline double WALLCLOCK() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec*1e-6;
}
#endif

// ============================================================================
// DATA MANIPULATION
// ============================================================================
// #define float2fintm(f) f ^ ((-(f >> 31)) | 0x80000000)
// #define fint2floatm(f) f ^ (((f >> 31) - 1) | 0x80000000)
#define GETBYTE(a, b) ((a >> (b << 3)) & 0xFF)
#define GETWORD(a, b) ((a >> (b << 4)) & 0xFFFF)
#define SWAP(TYPE, __x, __y) { register TYPE __tmp(__x); __x = __y; __y = __tmp; }
#define MAX(A, B) ((A)>(B)?(A):(B))

typedef __uint128_t uint128_t;

// ============================================================================
// IN-PLACE RADIX SORT
// ============================================================================
#define MIN_FOR_RADIX 64
#define USE_TEMPLATE 0

inline void inplaceInsertionSort(uint128_t *a, unsigned int n) {
	register unsigned i, j;
	register uint128_t k;
	for (i = 0; i != n; i++, a[j] = k)
		for (j = i, k = a[j]; j && (uint64_t) k < (uint64_t) a[j - 1]; j--)
			a[j] = a[j - 1];
}

inline void inplaceSelectionSort(uint128_t *a, unsigned int n) {
	register unsigned i, j;
	for (i = 0; i < n - 1; i++) {
		register unsigned k(i);
		for (j = i + 1; j < n; j++)
			if ((uint64_t) a[j] < (uint64_t) a[k])
				k = j;
		if (k != i)
			SWAP(uint128_t, a[k], a[i]);
	}
}

inline void selectionSort(uint128_t *a, uint128_t *out, unsigned int n) {
	register unsigned i, j, k;
	register uint64_t *ua = (uint64_t*) a;
	for (i = 0; i < n; i++) {
		for (j = i + 1, k = i; j < n; j++)
			if (ua[j * 2] < ua[k * 2])
				k = j;
		out[i] = a[k];
		if (k != i)
			a[k] = a[i];
	}
}

static void inplaceRadixSort(uint128_t *a, unsigned int n, int byte) {
	unsigned i, j, end;
	unsigned count[256] = { };

	for (i = 0; i < n; i++)
		count[GETBYTE(a[i], byte)]++;

	unsigned bucket[256];
	bucket[0] = 0;
	for (i = 1; i < 256; i++)
		bucket[i] = count[i - 1] + bucket[i - 1];

	for (i = end = 0; i < 256; i++) {
		if (count[i] > 0) {
			end += count[i];
			for (j = bucket[i]; j < end; j++) {
				register uint128_t value(a[j]);
				if (GETBYTE(value, byte) != i) {
					do {
						register unsigned xx(bucket[GETBYTE(value, byte)]++);
						SWAP(uint128_t, a[xx], value);
					} while (GETBYTE(value, byte) != i);
					a[j] = value;
				}
			}
			if (byte > 0) {
				if (count[i] < MIN_FOR_RADIX)
					inplaceSelectionSort(a + end - count[i], count[i]);
				else
					inplaceRadixSort(a + end - count[i], count[i], byte - 1);
			}
		}
	}
}

template<int byte>
inline void inplaceRadixSortByte(uint128_t *a, unsigned int n) {
	unsigned i, j, end;
	unsigned count[256] = { };

	for (i = 0; i < n; i++)
		count[GETBYTE(a[i], byte)]++;

	unsigned bucket[256];
	bucket[0] = 0;
	for (i = 1; i < 256; i++)
		bucket[i] = count[i - 1] + bucket[i - 1];

	for (i = end = 0; i < 256; i++) {
		if (count[i] > 0) {
			end += count[i];
			for (j = bucket[i]; j < end; j++) {
				register uint128_t value(a[j]);
				if (GETBYTE(value, byte) != i) {
					do {
						register unsigned xx(bucket[GETBYTE(value, byte)]++);
						SWAP(uint128_t, a[xx], value);
					} while (GETBYTE(value, byte) != i);
					a[j] = value;
				}
			}
			if (byte > 0) {
				if (count[i] < MIN_FOR_RADIX)
					inplaceSelectionSort(a + end - count[i], count[i]);
				else
					inplaceRadixSortByte<byte - 1>(a + end - count[i],
							count[i]);
			}
		}
	}
}

template<>
inline void inplaceRadixSortByte<0>(uint128_t *a, unsigned int n) {
	unsigned i, j, end;
	unsigned count[256] = { };

	for (i = 0; i < n; i++)
		count[a[i] & 0xFF]++;

	unsigned bucket[256];
	bucket[0] = 0;
	for (i = 1; i < 256; i++)
		bucket[i] = count[i - 1] + bucket[i - 1];

	for (i = end = 0; i < 256; i++) {
		end += count[i];
		for (j = bucket[i]; j < end; j++) {
			register uint128_t value(a[j]);
			if ((value & 0xFF) != i) {
				do {
					register unsigned xx(bucket[value & 0xFF]++);
					SWAP(uint128_t, a[xx], value);
				} while ((value & 0xFF) != i);
				a[j] = value;
			}
		}
	}
}

inline void sortArray(uint128_t *a, int n) {
	inplaceRadixSortByte<sizeof(uint64_t) - 1>(a, n);
}

#endif
