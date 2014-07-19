
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cctype>
#include <string>
#include <cassert>
#include <algorithm>
#include <boost/serialization/utility.hpp>
#include <boost/mpi.hpp>
using namespace std;
 
/*
 * There are N workers, rank 0 is the master
 * Assume rank 0 runs on master, and rank 1 - rank 8 runs on worker1, rank 9 - rank 16 runs on worker2
*/
string getFileName(int rank, const string& logFilesPath, const string&  jobName)
{
	char buf[128];
	sprintf(buf,"%d",rank - 1);
	return logFilesPath + jobName + "-machine" + string(buf) + "-output.txt";
}
long long parseLastNumber(const string& line)
{
	string number;
	for(int i = (int)(line.length()) - 1 ;i >= 0 ; i --)
	{
		if(isdigit(line[i]))
			number += line[i];
		else
			break;
	}
	reverse(number.begin(),number.end());
	return atoll(number.c_str());
}
long long getMessageNum(const string& file)
{
	ifstream fin(file.c_str());
	string line;
	while(getline(fin,line))
	{
		if(line.find("Message Transmission") != string::npos)
		{
			long long result = parseLastNumber(line);
			fin.close();
			return result;
		}
	}
	assert(false);
	return 0;
}
int main(int argc, char *argv[]) {
	boost::mpi::environment env;
	boost::mpi::communicator comm;

	const string logFilesPath = argv[1]; // /home/yanda/var/tmp/
	const string jobName = argv[2]; // hashmin-btc-lalp;
	const string output = argv[3];
	vector< pair<int, long long> > results;
	pair<int, long long> localresult;

	if(comm.rank() == 0)
	{
		boost::mpi::gather(comm, localresult, results, 0 );

		sort(results.begin(),results.end());
		long long sum = 0;
		ofstream fout(output.c_str());
		for(int i = 0 ;i < results.size() ; i++ )
		{
			sum += results[i].second;
			fout << results[i].first << "\t" << results[i].second << endl;
		}

		cout << "Total Messages: " << sum << endl;
		fout.close();
	}
	else
	{
		const string file = getFileName(comm.rank(),logFilesPath,jobName);
		long long messageNum = getMessageNum(file);
		localresult = make_pair(comm.rank(),messageNum);
		boost::mpi::gather(comm, localresult, results, 0 );
	}

	return 0;
}

