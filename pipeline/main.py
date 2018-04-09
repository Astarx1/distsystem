#!/usr/bin/env python2.7
###################################################################################################################################################
# Template written by David Cabinian
# dhcabinian@gatech.edu
# Written for python 2.7
# Run python template.py --help for information.
###################################################################################################################################################
# DO NOT MODIFY THESE IMPORTS / DO NOT ADD IMPORTS IN THIS NAMESPACE
# Importing a filesystem library such as ['sys', 'os', 'shutil'] will result in loss of all homework points.
###################################################################################################################################################
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
###################################################################################################################################################

#Index,Country,Description,Designation,Points,Price,Province,Region_1,Region_2,Variety,Winery,Quantity,User,DateTime

def bottles_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)

        def partition_csv(line):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            return (int(t[0]), int(t[11]))

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.Map(partition_csv)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def dollars_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)

        def partition_csv(line):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            return (int(t[0]), int(t[11])*int(t[5]))

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.Map(partition_csv)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def winery_bottles_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)

        def partition_csv(line):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            return (t[10], int(t[11]))

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.Map(partition_csv)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def winery_dollars_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)

        def partition_csv(line):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            return (t[10], int(t[11])*int(t[5]))

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.Map(partition_csv)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def variety_bottles_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)
        
        def filter_using_variety(line, variety):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            if t[9] == variety:
                yield (int(t[0]), int(t[11])) 

        def mapping(sale):
            return (sale[0], sale[1])

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.FlatMap(filter_using_variety, args.variety)
            | 'Mapping' >> beam.Map(mapping)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def variety_dollars_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)
        
        def filter_using_variety(line, variety):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            if t[9] == variety:
                yield (int(t[0]), int(t[5])*int(t[11])) 

        def mapping(sale):
            return (sale[0], sale[1])

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.FlatMap(filter_using_variety, args.variety)
            | 'Mapping' >> beam.Map(mapping)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def variety_winery_bottles_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)
        
        def filter_using_variety(line, variety):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            if t[9] == variety:
                yield (str(t[10]), int(t[11])) 

        def mapping(sale):
            return (sale[0], sale[1])

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.FlatMap(filter_using_variety, args.variety)
            | 'Mapping' >> beam.Map(mapping)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def variety_winery_dollars_sold(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)
        
        def filter_using_variety(line, variety):
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            if t[9] == variety:
                yield (str(t[10]), int(t[5])*int(t[11])) 

        def mapping(sale):
            return (sale[0], sale[1])

        def format(line):
            (word, count) = line
            return '%s\t%s' % (word, count)

        output = (lines 
            | 'Partition' >> beam.FlatMap(filter_using_variety, args.variety)
            | 'Mapping' >> beam.Map(mapping)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format))

        output | WriteToText(args.output)

def step2(args, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = p | ReadFromText(args.input)
        
        def mapping_using_time(line):   
            import re
            t = re.sub('"(.*?)"', '', line) 
            t = t.split(",")
            return (str(t[13]), int(t[0]))     

        def number_of_times_wines_have_been_linked(line):
            g = list(line[1])
            if len(g) == 1:
                yield ((str(g[0]), str("-1")), 1)
            for n in g:
                for m in g:
                    if n is not m:
                        yield ((str(n), str(m)), 1)

        def links_by_wine(line):
            return (line[0][0], (line[0][1], line[1]))

        def sort_wines_linked(line):
            wines_sorted = sorted(line[1], key=lambda x: x[1], reverse=True)
            return (line[0], wines_sorted)

        def only_keep_best_wines(line):
            w = []
            n = line[1][0][1]

            if line[1][0][0] == "-1":
                if len(line[1]) == 1:
                    return (line[0], [], 0)
                else:
                    n = line[1][1][1]
            for l in line[1]:
                if l[0] == "-1":
                    continue
                elif l[1] == n:
                    w.append(l[0])
                else:
                    break
            return (line[0], w, n)

        def format_output(line):
            return line[0] + "\t" + "\t".join(list(line[1])) + "\t" + str(line[2])

        output = (lines 
            | 'Partition' >> beam.Map(mapping_using_time)
            | 'GroupingPurchases' >> beam.GroupByKey())

        linked = (output
            | 'TimesLinkedByPurchase' >> beam.FlatMap(number_of_times_wines_have_been_linked)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)     
            | 'Separate' >> beam.Map(links_by_wine)
            | 'GroupingWines' >> beam.GroupByKey()
            | 'Sorting' >> beam.Map(sort_wines_linked)
            | 'BestLinks' >> beam.Map(only_keep_best_wines)
            | 'Formatting' >> beam.Map(format_output))

        linked | WriteToText(args.output, file_name_suffix='.csv', num_shards=1)


def run(args, pipeline_args):
    if args.purchased_together:
        step2(args, pipeline_args)
    else:
        if args.variety is not None:
            if args.bottles_sold:
                variety_bottles_sold(args, pipeline_args)
            elif args.dollars_sold:
                variety_dollars_sold(args, pipeline_args)
            elif args.winery_bottles_sold:
                variety_winery_bottles_sold(args, pipeline_args)
            elif args.winery_dollars_sold:
                variety_winery_dollars_sold(args, pipeline_args)
        else:
            if args.bottles_sold:
                bottles_sold(args, pipeline_args)
            elif args.dollars_sold:
                dollars_sold(args, pipeline_args)
            elif args.winery_bottles_sold:
                winery_bottles_sold(args, pipeline_args)
            elif args.winery_dollars_sold:
                winery_dollars_sold(args, pipeline_args)





###################################################################################################################################################
# DO NOT MODIFY BELOW THIS LINE
###################################################################################################################################################
if __name__ == '__main__':
    # This function will parse the required arguments for you.
    # Try template.py --help for more information
    # View https://docs.python.org/3/library/argparse.html for more information on how it works
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description="ECE 6102 Assignment 3", epilog="Example Usages:\npython test.py --input small_dataset.csv --output bottles.csv --runner Direct --bottles_sold\npython wine_test.py --input $BUCKET/input_files/small_dataset.csv --output $BUCKET/bottles.csv --runner DataflowRunner --project $PROJECT --temp_location $BUCKET/tmp/ --bottles_sold")
    parser.add_argument('--input', help="Input file to process.", required=True)
    parser.add_argument('--output', help="Output file to write results to.", required=True)
    parser.add_argument('--project', help="Your Google Cloud Project ID.")
    parser.add_argument('--runner', help="The runner you would like to use for the map reduce.", choices=['Direct', 'DataflowRunner'], required=True)
    parser.add_argument('--temp_location', help="Location where temporary files should be stored.")
    parser.add_argument('--num_workers', help="Set the number of workers for Google Cloud Dataflow to allocate (instead of autoallocation). Default value = 0 uses autoallocation.", default="0")
    pipelines = parser.add_mutually_exclusive_group(required=True)
    pipelines.add_argument('--bottles_sold', help="Count the total number of bottles sold for each wine that has been purchased at least once and order the final result from largest to smallest count.", action='store_true')
    pipelines.add_argument('--dollars_sold', help="Calculate the total dollar amount of sales for each wine and order the final result from largest to smallest amount.", action='store_true')
    pipelines.add_argument('--winery_bottles_sold', help="Count the total number of bottles sold for each winery that has had at least one bottle purchased and order the final result from largest to smallest count.", action='store_true')
    pipelines.add_argument('--winery_dollars_sold', help="Calculate the total dollar amount of sales for each winery and order the final result from largest to smallest amount.", action='store_true')
    pipelines.add_argument('--purchased_together', help="For each wine that was purchased at least once, find the other wine that was purchased most often at the same time and count how many times the two wines were purchased together.", action='store_true')
    parser.add_argument('--variety', help="Use the variety whose first letter is the closest to the first letter of your last name. NOTE: THE CHOICE \"Red Blend\" IS GIVEN FOR COMPARISON AGAINST THE GIVEN SOLUTION. DO NOT USE \"Red Blend\" FOR YOUR ASSIGNMENT SUBMISSION. To use \"Red Blend\" as a script argument place it in double quotes.", choices=["Chardonnay", "Malbec", "Pinot Noir", "Red Blend", "Riesling", "Sauvignon Blanc", "Zinfandel"])
    args = parser.parse_args()

    # Separating Pipeline options from IO options
    # HINT: pipeline args go nicely into: options=PipelineOptions(pipeline_args)
    if args.runner  == "DataflowRunner":
        if None in [args.project, args.temp_location]:
            raise Exception("Missing some pipeline options.")
        pipeline_args = []
        pipeline_args.append("--runner")
        pipeline_args.append(args.runner)
        pipeline_args.append("--project")
        pipeline_args.append(args.project)
        pipeline_args.append("--temp_location")
        pipeline_args.append(args.temp_location)
        if args.num_workers != "0":
            # This disables the autoscaling if you have specified a number of workers
            pipeline_args.append("--num_workers")
            pipeline_args.append(args.num_workers)
            pipeline_args.append("--autoscaling_algorithm") 
            pipeline_args.append("NONE")
    else:
        pipeline_args = []


    run(args, pipeline_args)