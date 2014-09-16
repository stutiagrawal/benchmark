import runBashCmd
import logging
import argparse
import os
import errno
import setupLog
import time
import multiprocessing

default_logger = logging.getLogger(name='benchmark')

def is_valid_input(cghub_key, outdir, logger=default_logger):
    
    if(not(os.path.isfile(cghub_key))):
        logger.error('Invalid key file path: %s' %(cghub_key))
        return False
    if(not(os.path.isdir(outdir))):
        logger.error('Invalid output dir: %s' %(outdir))
        return False
    return True

#Download the data
def download_bam(gt_path, uuid, outdir, cghub_key, logger=default_logger):

    if(is_valid_input(cghub_key, outdir)):
        print "Downloading %s" %(uuid)
        cmd = ['%s' %(os.path.join(gt_path, './gtdownload')), '-c', cghub_key, '-d', uuid, '-p', outdir]
        print cmd
        exitcode = runBashCmd._do_run(cmd)

        if exitcode != 0:
            msg = 'Download failed for %s' %(uuid)
            logger.error(msg)

#Convert it to fastq
def convert_to_fastq(inpdir, bam_file, uuid, picard_path, logger=default_logger):
    
    if(os.path.isdir(inpdir) and os.path.isdir(picard_path)):
        cmd = ['java','-Xmx1G', '-XX:-UseGCOverheadLimit', '-jar', '%s' %(os.path.join(picard_path,'SamToFastq.jar')),
                                        'VALIDATION_STRINGENCY=SILENT',
                                        'MAX_RECORDS_IN_RAM=null']
        inp = ['INPUT=%s' %(bam_file), 'FASTQ=%s' %(os.path.join(inpdir,'%s_1.fastq.gz' %(uuid))),
                'SECOND_END_FASTQ=%s' %(os.path.join(inpdir, '%s_2.fastq.gz' %(uuid)))]
        cmd = cmd + inp
        
        start_time = time.time()
        exitcode = runBashCmd._do_run(cmd)
        
        end_time = time.time()

        if exitcode != 0:
            msg = 'Conversion failed for: %s' %(bam_file)
            logger.error(msg)
        logger.info('PICARD_TIME:\t%s\t%s\t%s' %(bamfile, os.path.getsize(bamfile),
                                        float(end_time) - float(start_time)))
    else:
        logger.error('Invalid path %s or %s' %(inpdir, picard_path))

def GATK_snp_calling(reference, bamfile, outdir, program, GATK_path, logger=default_logger):
    """ Run GATk for SNP calling """

    if(os.path.isfile(reference) and os.path.isfile(bamfile) and
        os.path.isdir(outdir)):

        start_time = time.time()
        cmd = ['java','-Xmx7G', '-jar', '%s'%(os.path.join(GATK_path,'GenomeAnalysisTK.jar')), '-nt', '%s'%(int(0.8 * multiprocessing.cpu_count()))]
        inp = ['-R', reference, '-T', program, '-I', bamfile]
        out = ['-o', '%s' %(os.path.join(outdir, 'out_snps_%s' %(program)))]

        cmd = cmd + inp + out

        exitcode = runBashCmd._do_run(cmd)
        
        end_time = time.time()
        if exitcode != 0:
            msg = 'snp calling failed for %s' %(bamfile)
            logger.error(msg)
        logger.info('TIME:\t%s\t%s\t%s\t%s' %(bamfile, os.path.getsize(bamfile),
                                        float(end_time) - float(start_time),
                                        program))

    else:
        logger.error('Invalid reference, bam or output directory')

def align_bwa(dirname, uuid, reference, bwa_path, bamfile, logger=default_logger):
    """ Run bwa mem for alignment """

    if(os.path.isfile(reference) and os.path.isdir(dirname)):
        reads_1 = os.path.join(dirname, "%s_1.fastq.gz" %(uuid))
        reads_2 = os.path.join(dirname, "%s_2.fastq.gz" %(uuid))
        if(os.path.isfile(reads_1) and os.path.isfile(reads_2)):
            cmd = ['bwa', 'mem', '-t', '%s' %(int(0.8 * multiprocessing.cpu_count()))]
            inp = [reference, reads_1, reads_2]
            out = open(os.path.join(dirname, "bwa_%s.sam" %(uuid)), "w")

            cmd = cmd + inp

            start_time = time.time()
            exitcode = runBashCmd._do_run(cmd, output_file=out)
            end_time = time.time()

            out.close()
            if exitcode != 0:
                msg = '%s returned unexpected non-zero exitcode %d' %(cmd,
                                                                    exitcode)
                logger.error(msg)
            logger.info('BWA_TIME:\t%s\t%s\t%s' %(bamfile, os.path.getsize(bamfile),
                                        float(end_time) - float(start_time)))
        else:
            logger.info("Fastq files for UUID: %s not available" %(uuid))
    else:
        logger.error("Invalid reference %s or directory %s" %(reference, directory))

def getBAM(dirname, logger=default_logger):
    """ Function to get the path of a BAM file """

    if(os.path.isdir(dirname)):
        for filename in os.listdir(dirname):
            if (filename.endswith(".bam") and not(filename.startswith("novo"))):
                return os.path.join(dirname, filename)
    else:
        logger.error("Invalid directory: %s" %(dirname))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='benchmark.py', description='Benchmark GATK tools on the system')
    parser.add_argument('data', help='path to exome datasets')
    parser.add_argument('--bam', help='path to bam file for bechmarking')
    parser.add_argument('--ref', default='/glusterfs/netapp/homes1/STUTIA/GDC/ref_data/Homo_sapiens_assembly19.fasta',
    help='path to reference genome')
    parser.add_argument('--log_file', default=os.path.join(os.getcwd(),
                        "benchmark.log"), help='path/to/log/file')
    parser.add_argument('--GATK', default='/usr/bin/lib/GATK/',
                        help='path/to/GATK/jar/files')
    parser.add_argument('--filename', help='path to metadata from cghub browser')
    parser.add_argument('--picard', default='/usr/bin/lib/picard', help='path to picard binaries')
    parser.add_argument('--bwa_path', default='/usr/bin/lib/bwa', help='path to bwa binaries')
    parser.add_argument('--cghub_key', default='/usr/bin/keys/cghub.key', help='path to cghub key')    
    parser.add_argument('--gt_path', default='/home/ubuntu/software/cghub/libexec')
    args = parser.parse_args()

    logger = setupLog.setup_logging(logging.INFO, 'benchmark', args.log_file)

    #dirname = args.data
    
    fp = open(args.filename, "r")
    fp.readline()
    for line in fp:
        line = line.split("\t")
        uuid = line[16]
        dirname = os.path.join(args.data, uuid)
        has_fastq_files = False
        if not os.path.isdir(dirname):
                os.makedirs(dirname)
                download_bam(args.gt_path, uuid, dirname, args.cghub_key)
        if(os.path.isdir(dirname)):
            bamfile = getBAM(os.path.join(dirname, uuid))
            if(os.path.isfile(bamfile)):
                if(os.path.isfile('%s_1.fastq.gz' %(os.path.join(dirname, uuid))) 
                   and os.path.isfile('%s_2.fastq.gz' %(os.path.join(dirname, uuid)))):
                    has_fastq_files = True
                if(has_fastq_files):
                    align_bwa(dirname, uuid, args.ref, args.bwa_path, bamfile)
                else:
                    convert_to_fastq(dirname, bamfile, uuid, args.picard)
            #GATK_snp_calling(args.ref, bamfile, dirname, "UnifiedGenotyper",
            #                args.GATK)
            #GATK_snp_calling(args.ref, bamfile, dirname, "HaplotypeCaller",
            #                args.GATK)

#Align using BWA
#def align_bwa(reads_1, reads_2, outdir, 

#Align using Novoalign
