// Shivam Chourey
// shivam.chourey@gmail.com
// Parallel Merge-Sort Algorithm
// Assumptions: Input file consists of one word in each line
//              Input file - in.txt (the first one)
//              Only enough memory to load 3000 words at a time
// Hard-coded parts of code for these assumptions, could be generalized if needed

import java.io.*;
import java.util.*;

public class ParallelSorter
{
    public static void main(String[] args) throws Exception
    {
        File file = new File("in.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));

        // Divide file into pages with max 3000 words each(arbitrary) and
        // sort each page, write each sorted page on disk

        long TotalTime = 0;

        for(int pages = 1, counter  = 0; pages <= 2; pages++, counter = 0)
        {
            List<String> WordList = new ArrayList<>();	// For Dynamic size
            String Line;

            while((counter != 3000) && (Line = br.readLine()) != null)
            {
                WordList.add(Line);
                counter++;
            }

            // Print the Initial Word List
            System.out.println("\n The Original List read");

            // Sort the Word List
            long StartTime = System.nanoTime();
            ParallelMergeSortFunction(WordList, 4);
            long EndTime = System.nanoTime();
            TotalTime = TotalTime + (EndTime-StartTime);

            // Print the Sorted Word List
            write ("Out_Sub_"+pages+".txt", WordList, false);
            System.out.println("\n The Sorted List is created");
        }

        // Merge each sorted pages into a file
        // Load data from each page and sort, and write sorted data into an output file

        // Get buffer for each page
        File Page1 = new File("Out_Sub_1.txt");
        BufferedReader br1 = new BufferedReader(new FileReader(Page1));
        boolean EndOfPage1 = false;

        File Page2 = new File("Out_Sub_2.txt");
        BufferedReader br2 = new BufferedReader(new FileReader(Page2));
        boolean EndOfPage2 = false;

        int counter = 0;
        String Line;

        List<String> List1 = new ArrayList<>();	// For Dynamic size
        while((counter != 1500) && (Line = br1.readLine()) != null)
        {
            List1.add(Line);
            counter++;
        }

        counter = 0;

        List<String> List2 = new ArrayList<>();	// For Dynamic size
        while((counter != 1500) && (Line = br2.readLine()) != null)
        {
            List2.add(Line);
            counter++;
        }

        while ( !List1.isEmpty() || !List2.isEmpty() )
        {
            int MListSize = List2.size() + List1.size();
            List<String> MList = new ArrayList<>();
            for (int i = 0; i < MListSize; i++)
            {
                MList.add("");
            }

            // Merge the lists
            merge(MList, List1, List2, true, EndOfPage1, EndOfPage2);
            MList.removeAll(Arrays.asList("", null));  // Ignore the empty strings

            write ("out.txt", MList, true);

            // Empty the list, since all it's variables are written to file
            MList.removeAll(MList);

            // Load another set of words from pages
            Line = null;
            if(List1.size() < 1500)
            {
                counter = List1.size();
                while((counter != 1500) && (Line = br1.readLine()) != null)
                {
                    List1.add(Line);
                    counter++;
                }

                if(Line == null)
                    EndOfPage1 = true;
            }

            if(List2.size() < 1500)
            {
                counter = List2.size();
                while((counter != 1500) && (Line = br2.readLine()) != null)
                {
                    List2.add(Line);
                    counter++;
                }

                if(Line == null)
                    EndOfPage2 = true;
            }
        }

        // Print the time taken in execution
        PrintTime("out.txt", (TotalTime));
    }

    // Function that compares Alphabetical order of two strings
    // This function is technically a wrapper over Java's string function - compareTo
    public static int AlphabeticalComparison(String str1, String str2)
    {
        int var1 = str1.compareTo( str2 );
        return var1;
    }

    // Merge sort algorithm function - splits the list into left and right
    // Calls merge function for ordered merging of the left and right
    public static void mergeSort(List <String> WList)
    {
        int n  = WList.size();

        if (n < 2)
        {
            return;
        }

        int mid = n / 2;
        List <String> l = new ArrayList<>();
        List <String> r = new ArrayList<>();

        for (int i = 0; i < mid; i++)
        {
            l.add(WList.get(i));
        }
        for (int i = mid; i < n; i++)
        {
            r.add(WList.get(i));
        }
        mergeSort(l);
        mergeSort(r);

        merge(WList, l, r, false, false, false);
    }

    // Function to merge the two half lists
    public static void merge(List <String> WList, List <String> l, List <String> r, boolean FileMerging, boolean EP1, boolean EP2)
    {
        int k = 0;
        while (!l.isEmpty() && !r.isEmpty())
        {
            if ((AlphabeticalComparison(l.get(0), r.get(0))) <= 0)
            {
                WList.set(k++, l.get(0));
                l.remove(0);
            } else
            {
                WList.set(k++, r.get(0));
                r.remove(0);
            }
        }
        if (!FileMerging || (FileMerging && EP2))
        {
            while (!l.isEmpty())
            {
                WList.set(k++, l.get(0));
                l.remove(0);
            }
        }
        if (!FileMerging || (FileMerging && EP1))
        {
            while (!r.isEmpty())
            {
                WList.set(k++, r.get(0));
                r.remove(0);
            }
        }
    }

    public static void ParallelMergeSortFunction(List <String> WList, int NumberOfThreads)
    {
        // If no more threads left, use sequential sorting
        if(NumberOfThreads <= 1)
        {
            mergeSort(WList);
            return;
        }

        // Parallel sorting
        int n  = WList.size();
        if (n < 2)
        {
            return;
        }

        int mid = n / 2;
        List <String> l = new ArrayList<>();
        List <String> r = new ArrayList<>();

        for (int i = 0; i < mid; i++)
        {
            l.add(WList.get(i));
        }
        for (int i = mid; i < n; i++)
        {
            r.add(WList.get(i));
        }

        Thread LeftSorter = MergeSortParallel(l, NumberOfThreads);
        Thread RightSorter = MergeSortParallel(r, NumberOfThreads);

        LeftSorter.start();
        RightSorter.start();

        try
        {
          LeftSorter.join();
          RightSorter.join();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }

        merge(WList, l, r, false, false, false);
    }

    private static Thread MergeSortParallel(List <String> WList, int NumberOfThreads)
    {
        return new Thread()
        {
            @Override
            public void run()
            {
                ParallelMergeSortFunction(WList, NumberOfThreads/2);
            }
        };
    }

    // Function to write output file
    // Takes input string array
    public static void write (String filename, List <String> x, boolean append) throws IOException
    {
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(filename, append));

        String temp;
        for (int i = 0; i < x.size(); i++)
        {
            temp = x.get(i);
            outputWriter.write(temp+"\n");
        }

        outputWriter.flush();
        outputWriter.close();
    }

    public static void PrintTime(String filename, long TimeElapsed) throws IOException
    {
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(filename, true));

        outputWriter.write(TimeElapsed+" ns"+"\n");

        outputWriter.flush();
        outputWriter.close();
    }
}

