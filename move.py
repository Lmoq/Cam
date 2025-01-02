import os
import shutil
import time
from queue import Queue
from pathlib import Path
from threading import Thread, BoundedSemaphore
from zipfile import ZipFile as zf

sem = BoundedSemaphore(1)

class Movefile:
    
    def __init__( 
            self,
            src : Path,
            dst : Path,
            target_ext : list = None,
            ARCHIVE_F : bool = False,
            MOVE_F : bool = True):
        """
        Tracks file addition from source path and transfer to destination path.
        This will detect and transfer any kind of file,
        if not desired, specify the target_ext
        Set ARCHIVE_F to True to archive files upon transfer,
        the archive file is located inside dst path
        """
        self.source = src
        self.dest = dst
        
        self.src_set : set = None
        self.dst_set : set = None
        self.target_ext = target_ext
        
        self.running = True
        self.MOVE_F = MOVE_F
        self.ARCHIVE_F = ARCHIVE_F
        
        self.archiveQ = Queue()
        self.archive_name = "smtemp.zip"
        
        
    def getdir_set( self, path : str ) -> set:
        # Retrieve the list of files as set
        return set( os.listdir( path ) )
        
        
    def verify_paths( self ) -> bool:
        # Must be called first before self.run()
        src = self.source
        dst = self.dest
        
        if self.MOVE_F and not os.path.exists( src ):
            print( "Src path error" )
            return False
        
        if not os.path.exists( dst ):
            print( "Creating : dst ; archive" )
            os.mkdir( dst )
            
        archF = dst / self.archive_name    
        if not os.path.exists( archF ):
            print( "Creating archive" )
            
            with open( archF, "w" ) as f:
                pass
            
        # Gets a snapshot of the current files from src
        self.src_set = self.getdir_set( self.source )
        
        return True
        
        
    def manage_files( self ):
        while self.running:
            sem.acquire()
            # Retrive the added files using symmetric difference
            symdiff = self.src_set ^ self.getdir_set( self.source )
            
            if symdiff:
                for f in symdiff:
                    fp = Path( f )
                
                    if self.target_ext and not fp.suffix in self.target_ext:
                        continue
                    
                    newfile = self.source / f
                    newdest = self.dest / f
                
                    print(f"Moved : {newdest.name}")
                    shutil.move( newfile, newdest )
                    
                    if self.ARCHIVE_F:
                        self.archiveQ.put( newdest )
                    
            sem.release()
            time.sleep(0.5)
                
                
    def archive_files(self):
        while self.running and self.ARCHIVE_F:
            sem.acquire()
            
            while not self.archiveQ.empty():
                f = self.archiveQ.get()
                archive = self.dest / self.archive_name
                
                with zf( archive, "a" ) as z:
                    z.write( f, arcname = f.name )
                os.remove(f)
                print(f"Archived an item")
            
            sem.release()
            time.sleep(0.1)
            
            
    def run(self):
        t1 = Thread( target = self.manage_files )
        t2 = Thread( target = self.archive_files )
        
        t1.start()
        t2.start()
        
        t2.join()
        t1.join()
        
        
def main():
    # Set up paths
    # Test commit
    internal = Path( "/storage/emulated/0" )
    file_extension = [ ".jpg", ".mp4" ]
    
    src = internal / "DCIM/Camera"
    dst = internal / ".smtemp"
    
    bArchive = True
    bMove = True
    
    mv = Movefile( 
        src, 
        dst, 
        file_extension,
        bArchive,
        bMove )
        
    if mv.verify_paths() == False:
        print("Source & dest path error")
        return
    
    # Main loop
    try:
        mv.run()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt : Merry Rizzmas ")
        mv.running = False
    
    
if __name__ == '__main__':
    main()
    