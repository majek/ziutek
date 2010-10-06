from ziutek import rtftse
from ziutek.smalltable_extra import QListClient

def do(idx):
    fd = open('../enwiki-20081008-pages-articles.sentences-small-tweet', 'r')
    
    x = []
    j=0
    for i, line in enumerate(fd):
        x.append( (i, set(line.split())) )
        if len(x) == 4096:
            idx.put_multi(x)
            x = []
            j+=1
            if j % 3 == 0:
                idx.stats()
            if j == 30000/256: #was 30000
                break
    if x:
        idx.put_multi(x)
    fd.close()

def main():
    mc = QListClient("127.0.0.1:11211")
    idx = rtftse.Indexer(mc, block_size=512*1024, flush_delay=1200, max_tuples=512*1024)
    do(idx)
    idx.close()
    mc.close()
    print "bye!"

if False:
    if True:
        import hotshot
        prof = hotshot.Profile("stones.prof")
        prof.runcall(main)
        prof.close()
    else:
        import cProfile
        cProfile.run('main()', 'fooprof')
        import pstats
        p = pstats.Stats('fooprof')
        p.sort_stats('cumulative').print_stats(30)
        p.sort_stats('time').print_stats(30)
else:
    main()
