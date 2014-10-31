/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>

#include "mongo/base/initializer.h"
#include "mongo/db/json.h"
#include "mongo/tools/tool.h"
#include "mongo/tools/stat_util.h"
#include "mongo/util/text.h"

namespace po = boost::program_options;

namespace mongo {

    class TopTool : public Tool {
    public:

        TopTool() : Tool( "top" , REMOTE_SERVER , "admin" ) {
            _sleep = 1;

            add_hidden_options()
            ( "sleep" , po::value<int>() , "time to sleep between calls" )
            ;
            add_options()
            ( "locks" , "use db lock info instead of top" )
            ;
            addPositionArg( "sleep" , 1 );

            _autoreconnect = true;
        }

        virtual void printExtraHelp( ostream & out ) {
            out << "View live MongoDB collection statistics.\n" << endl;
        }
        
        bool useLocks() {
            return hasParam( "locks" );
        }

        NamespaceStats getData() {
            if ( useLocks() )
                return getDataLocks();
            return getDataTop();
        }

        NamespaceStats getDataLocks() {

            BSONObj out;
            if ( ! conn().simpleCommand( _db , &out , "serverStatus" ) ) {
                cout << "error: " << out << endl;
                return NamespaceStats();
            }

            return StatUtil::parseServerStatusLocks( out.getOwned() );
        }

        NamespaceStats getDataTop() {
            NamespaceStats stats;

            BSONObj out;
            if ( ! conn().simpleCommand( _db , &out , "top" ) ) {
                cout << "error: " << out << endl;
                return stats;
            }

            if ( ! out["totals"].isABSONObj() ) {
                cout << "error: invalid top\n" << out << endl;
                return stats;
            }

            out = out["totals"].Obj().getOwned();

            BSONObjIterator i( out );
            while ( i.more() ) {
                BSONElement e = i.next();
                if ( ! e.isABSONObj() )
                    continue;

                NamespaceInfo& s = stats[e.fieldName()];
                s.ns = e.fieldName();
                s.read = e.Obj()["readLock"].Obj()["time"].numberLong() / 1000;
                s.write = e.Obj()["writeLock"].Obj()["time"].numberLong() / 1000;
            }

            return stats;
        }
        
        void printDiff( const NamespaceStats& prev , const NamespaceStats& now ) {
            if ( prev.size() == 0 || now.size() == 0 ) {
                cout << "." << endl;
                return;
            }
            
            vector<NamespaceDiff> data = StatUtil::computeDiff( prev , now );
            
            unsigned longest = 30;
            
            for ( unsigned i=0; i < data.size(); i++ ) {
                const string& ns = data[i].ns;

                if ( ! useLocks() && ns.find( '.' ) == string::npos )
                    continue;

                if ( ns.size() > longest )
                    longest = ns.size();
            }
            
            int numberWidth = 10;

            cout << "\n"
                 << setw(longest) << ( useLocks() ? "db" : "ns" )
                 << setw(numberWidth+2) << "total"
                 << setw(numberWidth+2) << "read"
                 << setw(numberWidth+2) << "write"
                 << "\t\t" << terseCurrentTime()
                 << endl;
            for ( int i=data.size()-1; i>=0 && data.size() - i < 10 ; i-- ) {
                
                if ( ! useLocks() && data[i].ns.find( '.' ) == string::npos )
                    continue;

                cout << setw(longest) << data[i].ns 
                     << setw(numberWidth) << setprecision(3) << data[i].total() << "ms"
                     << setw(numberWidth) << setprecision(3) << data[i].read << "ms"
                     << setw(numberWidth) << setprecision(3) << data[i].write << "ms"
                     << endl;
            }

        }

        int run() {
            _sleep = getParam( "sleep" , _sleep );

            if (isMongos()) {
                log() << "mongotop only works on instances of mongod." << endl;
                return EXIT_FAILURE;
            }

            NamespaceStats prev = getData();

            while ( true ) {
                sleepsecs( _sleep );
                
                NamespaceStats now;
                try {
                    now = getData();
                }
                catch ( std::exception& e ) {
                    cout << "can't get data: " << e.what() << endl;
                    continue;
                }

                if ( now.size() == 0 )
                    return -2;
                
                try {
                    printDiff( prev , now );
                }
                catch ( AssertionException& e ) {
                    cout << "\nerror: " << e.what() << endl;
                }

                prev = now;
            }

            return 0;
        }

    private:
        int _sleep;
    };

}

int toolMain( int argc , char ** argv, char ** envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    mongo::TopTool top;
    return top.main( argc , argv );
}

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF-16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables toolMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    mongo::WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = toolMain(argc, wcl.argv(), wcl.envp());
    ::_exit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = toolMain(argc, argv, envp);
    ::_exit(exitCode);
}
#endif
