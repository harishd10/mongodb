#ifndef TIMING_HPP
#define TIMING_HPP

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

#endif
