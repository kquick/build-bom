/// This module provides functionality for running one or more sub-process
/// operations.  Each sub-process operation is specified as the command to run,
/// the arguments to the command, and the input and output files.
///
/// The input and output files can be supplied to the command in a number of
/// ways: by replacing a pattern in one or more of the args, or by simply
/// appending the file to the list of arguments (if both input and output files
/// are marked this way, the input file(s) are appended first, followed by the
/// output file.
///
/// There can also be a chain of operations which are performed in sequential
/// order.  The assumption is that each subsequent operation consumes the output
/// of the previous operation (i.e. the out_file of an operation becomes the
/// inp_file of the next operation) and this linkage is automatically setup when
/// executing the chain.  This is especially useful when the input files are
/// specified as NamedFile::Temp files in which case the chain is provided with
/// the original input file that starts the chain and the final output file that
/// the chain should produce and it can automatically generate the intermediary
/// files as temporary files.
///
/// To facilitate the generation of a chain of commands where the individual
/// commands may or may not actually be executed as part of the final chain, each
/// chained command can be enabled or disabled, where the latter effectively
/// erases it from the actually executed chain, but while the chain is built the
/// arguments can still be specified for that operation (allowing the calling
/// code to avoid a series of conditional updates).
///
/// ** Structures, Traits, and their relationships:
///
/// These are described below in more detail, but this is an overview to help
/// visualize the relationships.
///
///  * Fundamentally, this module is designed to allow one or more executables to
///    be run (each in its own sub-process), where each executable has zero or
///    more input files and an output file.
///
///  * An Executable can be described in a general manner: the executable file,
///    standard arguments, and the manner in which input and output files should
///    be specified via additional arguments.
///
///  * A specific operation (SubProcOperation) is created by referencing the
///    generic executable information and the specific files to use as input and
///    output, along with any additional arguments for that specific operation.
///
///  * A ChainedOp allows a sequence of multiple specific operations to be run,
///    the typical mode is that the output file from a previous operation becomes
///    the input file to a subsequent operation.  Also in this mode, it is
///    convenient to specify that intermediate files in the chain should be
///    temporary files that are automatically removed upon completion of the
///    execution.
///
///  * There is also a FunctionOperation that allows running a local function to
///    convert input file(s) to output files.  This is convenient to use in
///    ChainedOp sequences for steps that are local computations rather than
///    external executables.
///
///  * The RunnableOp serves as a wrapper to contain the other types of
///    operation; higher level functionality is written in terms of a RunnableOp
///    type that will dispatch trait operations to the specific operation type's
///    trait handler.
///
///
///     File Handling                          Generic Description
///    --------------------------              -------------------
///
///     NamedFile:                             Executable:
///      ^    Temp("seed")                      ^         exe_file: PathBuf
///      |    Actual(PathBuf)                   |         base_args: Vec<String>
///      |    Glob("glob")                      |   +--<- inp_file
///      |                                      |   +--<- exe_file
///      |  FileTransformation: <------------+  |   |
///      +--<- inp_filename  ^........       |  |  ExeFileSpec:
///      +--<- out_filename          :       |  |     NoFileUsed
///            in_dir                :       |  |     Append
///                                  :       |  |     Option("-opt")
///       [trait] FilesPrep: ........:....   |  |     ViaCall(fn)
///                 set_input_file       :   |  |
///                 set_output_file      :   |  |
///                 set_dir              :   |  |  Specific Operations
///                                      :   |  |  -------------------
///                                      :   |  |
///                                      :...|..|........> SubProcOperation:
///                                      :   |  +------------<- exec   ^  ^
///                                      :   |                  args   |  :
///     DesignatedFile:                  :   +---------------<- files  |  :
///      ^        NoDesignatedFile       :   |                         |  :
///      |  +-<-- SingleFile             :...|..> FunctionOperation:   |  :
///      |  +-<-* MultiFile              :   |        name       ^^....|..:
///      |  |                            :   |        call: fn   |     |  :
///      |  v                            :   +-----<- files      |     |  :
///      | FileRef:                      :   |                   |     |  :
///      |    StaticFile(PathBuf)        :...|.....v       v.....|.....|..:
///      |    TempFile(*active mgmt*)    :   |     RunnableOp:   |     |  :
///      |                               :   |     ^   Call ->---+     |  :
///      |                               :   |     |   Exec ->---------+  :
///      |                               :   |     |                      :
///      |                               :...|.....|...v       v..........:
///      |                                   |     |   ChainedOps:        :
///      |                                   |     |     <ChainedIntOps>  :
///      |                                   |     +----<-* chain         :
///      |                                   +------------- files         :
///      |                                                                :
///      |               [trait] OpInterface: ............................:
///      +------------------------- execute()
///                                 execute_with_inp_override()
///                                 execute_with_file_overrides()
///
/// ----------------------------------------------------------------------
/// Alternatives:
///
/// * subprocess crate (https://crates.io/crates/subprocess)
///
///     The subprocess crate allows creation of pipelines connected via
///     stdin/stdout, but not sequences using shared input/output files.
///
///     In addition, chainsop provides automatic creation and management of
///     temporary files used in the above.
///
///     The chainsop package provides more direct support for incrementally
///     building the set of commands with outputs; the subprocess crate would
///     require more discrete management and building of a Vec<Exec>.
///
///     The chainsop allows elements of the chain to be local functions called in
///     the proper sequence of operations and for elements of the chain to be
///     disabled prior to actual execution (where they are skipped).
///
///     The subprocess crate provides more features for handling stdout/stderr
///     redirection, non-blocking and timed sub-process waiting, and interaction
///     with the sub-process.
///
///     Summary: significant overlap in capabilities with slightly different
///     use-case targets and features.
///
/// * duct (https://github.com/oconner663/duct.rs
///
///     Lightweight version of the subprocess crate
///
/// * cargo-make, devrc, rhiz, run-cli, naumann, yamis
///
///    Task runners, requiring an external specification of the commands and no
///    support for chaining inputs/outputs.  These could be written on top of
///    chainsop.
///
/// * steward crate (https://crates.io/crates/steward)
///
///    Useful for running multiple commands and allows dependency management, but
///    not input/output chaining or incremental command building.  Does support
///    other features like environment control and process pools.  Closer to
///    chainsop than the task runners, but again, this could be written on top of
///    chainsop.

use std::cell::{RefCell, RefMut};
use std::env::current_dir;
use std::ffi::{OsString};
use std::fmt;
use std::path::{Path,PathBuf};
use std::process;
use std::rc::Rc;


// ----------------------------------------------------------------------

/// This is the core definition of an operation that will be run.  This can be
/// considered to be the template: a generic specification of describing the
/// target executable.  To actually run the defined operation in a specific
/// scenario, a SubProcOperation should be initialized from it and customized
/// with the needed operational parameters.
#[derive(Debug,Clone)]
pub struct Executable {
    pub exe_file : PathBuf,
    base_args : Vec<String>,
    inp_file : ExeFileSpec,
    out_file : ExeFileSpec,
}

/// Specifies the manner in which a file is provided to an Executable command
#[derive(Clone,Default)]
pub enum ExeFileSpec {
    /// No file provided or needed
    NoFileUsed,

    /// Append the file to the command string.  If both the input and the output
    /// file are specified in this manner, the input file is provided before the
    /// output file.
    #[default]
    Append,

    /// Specify the file using this option, which will be followed by the file as
    /// the next argument (e.g. Option("-f") to specify "CMD -f FILE").
    Option(String),

    /// The file is added to the arguments list by a special function.  The
    /// function specified here is called with the argument list and the named
    /// file; it should add the named file to the arguments list in some manner
    /// appropriate to the command.
    ViaCall(Rc<dyn Fn(&mut Vec<OsString>, &DesignatedFile) -> anyhow::Result<()>>),
}

impl fmt::Debug for ExeFileSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExeFileSpec::NoFileUsed => "".fmt(f),
            ExeFileSpec::Append => "append".fmt(f),
            ExeFileSpec::Option(o) => format!("option({})", o).fmt(f),
            ExeFileSpec::ViaCall(_) => "via function call".fmt(f),
        }
    }
}

impl ExeFileSpec {

    /// Constructs the Option ExeFileSpec with automatic argument conversion
    pub fn option<'a, T: ?Sized>(optname : &'a T) -> ExeFileSpec
    where T: ToString
    {
        ExeFileSpec::Option(optname.to_string())
    }
}

impl Executable {

    /// Creates a new Executable to describe how to execute the corresponding
    /// process command and how that command is provided with input and output
    /// filenames.
    pub fn new<'a, T: ?Sized>(exe : &'a T,
                              inp_file : ExeFileSpec,
                              out_file : ExeFileSpec)
                              -> Executable
    where &'a T: Into<PathBuf>
    {
        Executable {
            exe_file : exe.into(),
            base_args : Vec::new(),
            inp_file : inp_file.clone(),
            out_file : out_file.clone(),
        }
    }

    /// Adds a command-line argument to use when executing the command.
    #[inline]
    pub fn push_arg<T>(&self, arg: T) -> Executable
    where T: Into<String>
    {
        Executable {
            base_args : { let mut tmp = self.base_args.clone();
                          tmp.push(arg.into());
                          tmp
            },
            ..self.clone()
        }
    }

    /// Specifies the name of the executable file
    #[inline]
    pub fn set_exe<T>(&self, exe: T) -> Executable
    where T: Into<PathBuf>
    {
        Executable {
            exe_file : exe.into(),
            ..self.clone()
        }
    }

}

// Definitions for common Executable commands.

// KWQ add _EXE or something to these to clarify they are Executable?

pub fn c_compile() -> Executable
{
    Executable::new("cc", ExeFileSpec::Append, ExeFileSpec::option("-o"))
}

pub fn clang() -> Executable
{
    Executable::new("clang", ExeFileSpec::Append, ExeFileSpec::option("-o"))
}

// pub fn tar_extract() -> Executable
// {
//     Executable::new("tar",
//                     ExeFileSpec::option("-f"),
//                     ExeFileSpec::NoFileUsed)
//         .push_arg("-x")
// }

// lazy_static::lazy_static! {
//     // pub static ref ADD_ELF_SECTION: Executable =
//     //     Executable::new("objcopy",
//     //                     ExeFileSpec::replace_token("INPFILE"),
//     //                     ExeFileSpec::Append)
//     //     .push_arg("--add-section")
//     //     .push_arg("
//  }

pub fn clang_bitcode() -> Executable
{
    Executable::new("clang", ExeFileSpec::Append, ExeFileSpec::option("-o"))
        .push_arg("-c")
        .push_arg("-emit-llvm")
}


// ----------------------------------------------------------------------
// Specific application of Executable: SubProcOperation on NamedFile(s)

/// Designates a type of file that can be identified by name on the command line.
#[derive(Clone, Debug)]
pub enum NamedFile {
    /// Create a temporary file; str is suffix to give temporary filename
    Temp(String),

    /// Actual filename (may or may not currently exist)
    Actual(PathBuf),

    // Multiple input files: not yet supported.  It is ostensibly better to
    // represent them here because all input files should share the same FileSpec
    // enum type, but multiple output files isn't really supported for chaining
    // to the next input...
    //
    // Actuals(Vec<PathBuf>),

    /// glob search in specified dir for all matching files
    GlobIn(PathBuf, String),

    /// allowed on initial construction, but an error for execute
    TBD    // KWQ removable?
}

impl NamedFile {
    /// Generates the designation indicating the need for a temporary file with
    /// the specified suffix.  If no particular suffix is needed, a blank suffix
    /// value should be specified.
    pub fn temp<T>(suffix: T) -> NamedFile
    where T: Into<String>
    {
        NamedFile::Temp(suffix.into())
    }

    /// Generates a reference to an actual file
    pub fn actual<T>(fpath: T) -> NamedFile
    where T: Into<PathBuf>
    {
        NamedFile::Actual(fpath.into())
    }

    /// Generates a reference to files identified by a file-globbing
    /// specification.
    pub fn glob_in<T,U>(dpath: T, glob: U) -> NamedFile
    where T: Into<PathBuf>, U: Into<String>
    {
        NamedFile::GlobIn(dpath.into(), glob.into())
    }
}


fn with_globbed_matches<Do>(in_dir: &PathBuf, for_glob: &String, mut do_with: Do)
                            -> anyhow::Result<()>
where Do: FnMut(&Vec<PathBuf>) -> anyhow::Result<()>
{
    let mut globpat = String::new();
    globpat.push_str(&OsString::from(in_dir).into_string().unwrap());
    globpat.push_str("/");
    globpat.push_str(for_glob);
    let glob_files = glob::glob(&globpat)?.filter_map(Result::ok).collect();
    do_with(&glob_files)
}


// ----------------------------------------------------------------------

#[derive(Clone)]
struct FileTransformation {
    inp_filename : NamedFile,
    out_filename : NamedFile,
    in_dir : Option<PathBuf>,
}

impl std::fmt::Debug for FileTransformation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        format!("transforming {:?} into {:?} in {:?}",
                self.inp_filename,
                self.out_filename,
                self.in_dir)
            .fmt(f)
    }
}

impl FileTransformation {
    pub fn new() -> FileTransformation {
        FileTransformation {
            inp_filename : NamedFile::TBD,
            out_filename : NamedFile::TBD,
            in_dir : None,
        }
    }
}

/// Resolves a FileSpec and insert the actual named file into the argument
/// list.  This also returns the file; the file may be a temporary file
/// object which will delete the file at the end of its lifetime, so the
/// returned value should be held until the file is no longer needed.
fn setup_file<E>(candidate: &NamedFile,
                 on_missing: E) -> anyhow::Result<DesignatedFile>
where E: Fn() -> anyhow::Result<DesignatedFile>
{
    match candidate {
        NamedFile::TBD => on_missing(),
        NamedFile::Temp(sfx) => {
            let tf = tempfile::Builder::new().suffix(sfx).tempfile()?;
            Ok(DesignatedFile::SingleFile(FileRef::TempFile(tf)))
        }
        NamedFile::Actual(fpath) => {
            Ok(DesignatedFile::SingleFile(FileRef::StaticFile(fpath.clone())))
        }
        NamedFile::GlobIn(dpath, glob) => {
            let mut fpaths = Vec::new();
            with_globbed_matches(
                dpath, glob,
                |files| {
                    for each in files {
                        fpaths.push(FileRef::StaticFile(each.clone()));
                    };
                    Ok(())
                })?;
            Ok(DesignatedFile::MultiFile(fpaths))
        }
    }
}

fn setup_files(candidates: &Vec<PathBuf>,
               default: &Option<NamedFile>) -> anyhow::Result<DesignatedFile>
{
    if candidates.len() == 0 {
        match default {
            Some(df) =>
                return setup_file(df, || Ok(DesignatedFile::NoDesignatedFile)),
            None => (),
        }
    }
    let mut files = DesignatedFile::NoDesignatedFile;
    for cf in candidates {
        let f = setup_file(&NamedFile::Actual(cf.clone()),
                           || Ok(DesignatedFile::NoDesignatedFile))?;
        files = files.extend(f)?;
    };
    Ok(files)
}


pub trait FilesPrep {

    /// Sets the directory from which the operation will be performed.  The
    /// caller is responsible for ensuring any Actual NamedFile paths are valid
    /// when operating from that directory and any Temp NamedFile FileRef files
    /// created will still be created in the normal temporary directory location.
    fn set_dir<T>(&mut self, tgtdir: T) -> &mut Self
    where T: AsRef<Path>;

    /// Sets the input file for the operation, overriding any previous input file
    /// specification.
    fn set_input_file(&mut self, fname: &NamedFile) -> &mut Self;

    /// Sets the output file for the command, overriding any previous output file
    /// specification.
    fn set_output_file(&mut self, fname: &NamedFile) -> &mut Self;
}

impl FilesPrep for FileTransformation {
    fn set_dir<T>(&mut self, tgtdir: T) -> &mut Self
    where T: AsRef<Path>
    {
        self.in_dir = Some(tgtdir.as_ref().to_path_buf());
        self
    }

    fn set_input_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.inp_filename = fname.clone();
        self
    }

    // KWQ remove the _file suffix again so that these are more general (e.g. could take a stdout or stderr repr)... or just keep the _file due to Unix view of everything as a file?

    fn set_output_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.out_filename = fname.clone();
        self
    }

}


// ----------------------------------------------------------------------
// Single sub-process operation management

/// Defines the interface for an Operation that can be performed (where an
/// operation is something like running an executable in a subprocess or calling
/// a local function to process a file).
pub trait OpInterface : FilesPrep {

    /// Returns a short identifier of this operation, usually used for
    /// user-presented identification purposes.
    fn label(&self) -> &str;

    /// Adds an argument to use when executing the operation.  This can, for
    /// example, be used for specifying command-line option arguments when
    /// running a subprocess Executable operation.  Each operation type and
    /// instance can determine how it will handle any specified arguments.
    fn push_arg<T>(&mut self, arg: T) -> &mut Self
    where T: Into<OsString>;

    /// Executes this command in a subprocess in the specified directory.  The
    /// input and output files will be determined and added to the command-line
    /// as indicated by their NamedFile values.  The successful result specifies
    /// the output file written (if any).
    ///
    /// The specified directory in which to execute the command is specified by
    /// the input cwd parameter; if the directory for this SubProcOperation has
    /// been explicitly overridden by calls to SubProcOperation::set_dir() (or
    /// ChainedOpRef::set_dir()) then those take priority and this input cwd is
    /// ignored.  This is useful for setting a default directory, but allowing a
    /// particular operation to explicitly override the directory.
    fn execute(&self, cwd: &Path, echo : bool) -> anyhow::Result<DesignatedFile>;

    /// Performs this operation in the specified directory, overriding the input.
    /// There might be multiple input files (e.g. with GlobIn): the NamedFile
    /// application is repeated for each input file.  If there are no input
    /// files, then this behaves just as the normal execute function.
    fn execute_with_inp_override(&self,
                                 cwd: &Path,
                                 inps: &Vec<PathBuf>,
                                 echo : bool)
                                 -> anyhow::Result<DesignatedFile>;

    /// Performs this operation in the specified directory, overriding the input
    /// *and* the output files.  If the output override is None this acts the
    /// same as execute_with_inp_override.
    fn execute_with_file_overrides(&self,
                                   cwd: &Path,
                                   inps: &Vec<PathBuf>,
                                   out: &Option<PathBuf>,
                                   echo : bool)
                                   -> anyhow::Result<DesignatedFile>;

}


#[derive(Debug)]
pub enum RunnableOp {
    Exec(SubProcOperation),
    Call(FunctionOperation)
}

impl FilesPrep for RunnableOp {
   fn set_dir<T>(&mut self, tgtdir: T) -> &mut Self
    where T: AsRef<Path>
    {
        match *self {
            Self::Exec(ref mut sp) => { sp.set_dir(tgtdir); },
            Self::Call(ref mut fp) => { fp.set_dir(tgtdir); },
        };
        self
    }

    fn set_input_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        match *self {
            Self::Exec(ref mut sp) => { sp.set_input_file(fname); },
            Self::Call(ref mut fp) => { fp.set_input_file(fname); },
        };
        self
    }

    fn set_output_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        match *self {
            Self::Exec(ref mut sp) => { sp.set_output_file(fname); },
            Self::Call(ref mut fp) => { fp.set_output_file(fname); },
        };
        self
    }
}

// KWQ NamedFile --> FileArgument ?

impl OpInterface for RunnableOp {

    fn label(&self) -> &str {
        match self {
            Self::Exec(sp) => sp.label(),
            Self::Call(fp) => fp.label(),
        }
    }

    fn push_arg<T>(&mut self, arg: T) -> &mut Self
    where T: Into<OsString>
    {
        match *self {
            Self::Exec(ref mut sp) => { sp.push_arg(arg); },
            Self::Call(ref mut fp) => { fp.push_arg(arg); },
        };
        self
    }

    fn execute(&self, cwd: &Path, echo : bool) -> anyhow::Result<DesignatedFile>
    {
        match self {
            Self::Exec(sp) => sp.execute(cwd, echo),
            Self::Call(fp) => fp.execute(cwd, echo),
        }
    }

    fn execute_with_inp_override(&self,
                                 cwd: &Path,
                                 inps: &Vec<PathBuf>,
                                 echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        match self {
            Self::Exec(sp) => sp.execute_with_inp_override(cwd, inps, echo),
            Self::Call(fp) => fp.execute_with_inp_override(cwd, inps, echo),
        }
    }

    fn execute_with_file_overrides(&self,
                                   cwd: &Path,
                                   inps: &Vec<PathBuf>,
                                   out: &Option<PathBuf>,
                                   echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        match self {
            Self::Exec(sp) => sp.execute_with_file_overrides(cwd, inps, out, echo),
            Self::Call(fp) => fp.execute_with_file_overrides(cwd, inps, out, echo),
        }
    }
}

/// This structure represents a single command to run as a sub-process, the
/// command's arguments, and the input and output files for that sub-process.
/// The structure itself is public but the fields are private
/// (i.e. implementation specific); the impl section below defines the visible
/// operations that can be performed on this structure.
#[derive(Clone,Debug)]
pub struct SubProcOperation {
    exec : Executable,
    args : Vec<OsString>,
    files : FileTransformation,
}


/// This structure represents a single command that is performed via a local code
/// function instead of running an Executable in a SubProcOperation.  This can be
/// used for operations in the Chain that the local program performs and avoids
/// the need to create multiple chains around this functionality.  For
/// example, a chain of operations that midway through creates a tar file
/// could use a SubProcOperation with the Executable("tar") or it could use a
/// FunctionOperation that uses the rust `tar::Builder` to generate the tar
/// file via rust functionality.
///
/// The first argument to the called function is the reference directory, the
/// second is the input file(s) that should be processed, and the last is the
/// output file that should be generated.
///
/// The reference directory would be the current directory for the command had it
/// been a SubProcOperation. The actual current directory for this process is
/// *not* set to this reference directory; handling of the reference directory is
/// left up to the called function.
#[derive(Clone)]
pub struct FunctionOperation {
    name : String,  // for informational purposes only
    call : Rc<dyn Fn(&Path, &DesignatedFile, &DesignatedFile) -> anyhow::Result<()>>,
               // n.b. Would prefer this to be an FnOnce, but that breaks move
               // semantics when trying to call it while it's a part of an
               // enclosing Enum.
    files : FileTransformation,
}

impl std::fmt::Debug for FunctionOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        format!("Local function call '{}' {:?}", self.name, self.files).fmt(f)
    }
}

impl FunctionOperation {

    /// Creates a new FunctionOperation that will call a local function instead of
    /// executing a command in a sub-process.  This is useful for interleaving
    /// local processing into the command chain where that local processing is
    /// executed in proper sequence with the other commands.  The local function
    /// is provided with the "argument list" that would have been passed on the
    /// command-line; this argument list will contain any input or output
    /// filenames that should be used by the function.
    ///
    /// A local function execution in the chain can only pass an output file to
    /// the subsequent operation in the chain; more complex data exchange would
    /// need to be serialized into that output file and appropriately consumed by
    /// the next stage. This might initially seem awkward, but makes sense when
    /// you consider that most operations are executions in subprocesses that are
    /// in a separate address space already.
    pub fn calling<T>(n: &str, f: T) -> FunctionOperation
    where T: Fn(&Path, &DesignatedFile, &DesignatedFile) -> anyhow::Result<()> + 'static
    {
        FunctionOperation {
            name : n.to_string(),
            call : Rc::new(f),
            files : FileTransformation::new(),
        }
    }

    fn run_with_files(&self,
                      cwd: &Path,
                      inpfiles: DesignatedFile,
                      outfile: DesignatedFile,
                      echo: bool) -> anyhow::Result<DesignatedFile> {
        let fromdir = self.files.in_dir.clone().unwrap_or_else(|| cwd.to_path_buf());
        if echo {
            println!("Call {:?}, input={:?}, output={:?} [in {:?}]",
                     self, inpfiles, outfile, fromdir);
        }
        (self.call)(&fromdir, &inpfiles, &outfile)?;
        Ok(outfile)
    }
}

impl FilesPrep for FunctionOperation {
    fn set_dir<T>(&mut self, tgtdir: T) -> &mut Self
    where T: AsRef<Path>
    {
        self.files.set_dir(tgtdir);
        self
    }

    fn set_input_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.files.set_input_file(fname);
        self
    }

    fn set_output_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.files.set_output_file(fname);
        self
    }
}

impl OpInterface for FunctionOperation {

    fn label(&self) -> &str { &self.name }

    fn push_arg<T>(&mut self, arg: T) -> &mut Self
    where T: Into<OsString>
    {
       panic!("Should a FunctionOperation allow args?");
    }

    fn execute(&self, cwd: &Path, echo : bool) -> anyhow::Result<DesignatedFile>
    {
        let inpfiles = setup_file(&self.files.inp_filename,
                                  || Ok(DesignatedFile::NoDesignatedFile),
        )?;
        let outfile = setup_file(&self.files.out_filename,
                                 || Ok(DesignatedFile::NoDesignatedFile),
        )?;
        self.run_with_files(cwd, inpfiles, outfile, echo)
    }

    fn execute_with_inp_override(&self,
                                 cwd: &Path,
                                 inps: &Vec<PathBuf>,
                                 echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        let inpfiles = setup_files(inps, &None)?;
        let outfile = setup_file(&self.files.out_filename,
                                 || Ok(DesignatedFile::NoDesignatedFile),
        )?;
        self.run_with_files(cwd, inpfiles, outfile, echo)
    }

    fn execute_with_file_overrides(&self,
                                   cwd: &Path,
                                   inps: &Vec<PathBuf>,
                                   out: &Option<PathBuf>,
                                   echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        match out {
            None => self.execute_with_inp_override(cwd, inps, echo),
            Some(outf) => {
                let inpfiles = setup_files(inps, &None)?;
                let outfile = setup_file(&NamedFile::Actual(outf.clone()),
                                         || Ok(DesignatedFile::NoDesignatedFile),
                )?;
                self.run_with_files(cwd, inpfiles, outfile, echo)
            }
        }
    }
}


/// The DesignatedFile is the actual file(s) to use for the input or output of a
/// SubProcOperation or FunctionOperation.  It is constructed from the NamedFile
/// information provided to the overall operation and refers to 0 or more actual
/// (or intended actual) files (each of which is identified by the underlying
/// FileRef object contained in the DesignatedFile).
#[derive(Debug)]
pub enum DesignatedFile {    // KWQ --> SpecificFile ??
    NoDesignatedFile,
    SingleFile(FileRef),
    MultiFile(Vec<FileRef>),
}


/// The FileRef is a reference to a single file, with possible resource
/// management scope and responsibilities.
#[derive(Debug)]
pub enum FileRef {
    StaticFile(PathBuf),
    TempFile(tempfile::NamedTempFile)
}

    // KWQ: if main build attempt is c compiler, but it fails, don't try to generate bitcode in the background...

impl DesignatedFile {

    fn extend(self, more: DesignatedFile) -> anyhow::Result<DesignatedFile>
    {
        match self {
            DesignatedFile::NoDesignatedFile => Ok(more),
            DesignatedFile::SingleFile(pb) =>
                match more {
                    DesignatedFile::NoDesignatedFile => Ok(DesignatedFile::SingleFile(pb)),
                    DesignatedFile::SingleFile(pb2) => Ok(DesignatedFile::MultiFile(vec![pb, pb2])),
                    DesignatedFile::MultiFile(mut pbs) =>
                        Ok(DesignatedFile::MultiFile({pbs.push(pb); pbs})),
                },
            DesignatedFile::MultiFile(mut pbs) =>
                match more {
                    DesignatedFile::NoDesignatedFile => Ok(DesignatedFile::MultiFile(pbs)),
                    DesignatedFile::SingleFile(pb2) =>
                        Ok(DesignatedFile::MultiFile({pbs.push(pb2); pbs})),
                    DesignatedFile::MultiFile(pbs2) =>
                        Ok(DesignatedFile::MultiFile({pbs.extend(pbs2); pbs})),
                }
        }
    }

    /// Gets the Path associated with a DesignatedFile or returns an error if there
    /// is no Path.  This expects there to be a single path and will generate an
    /// error if there are multiples. The `cmd` and `path_of` arguments are
    /// informative for error generation.
    pub fn to_path(&self, op_label: &str, to_what: &str) -> anyhow::Result<PathBuf>
    {
        match self {
            DesignatedFile::SingleFile(fref) => Ok(Self::getPath(fref)),
            DesignatedFile::NoDesignatedFile =>
                Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(op_label.into(),
                                                   to_what.into()))),
            DesignatedFile::MultiFile(_) =>
                Err(anyhow::Error::new(
                    SubProcError::ErrorUnsupportedDesignatedFile(op_label.into()))),
        }
    }

    fn getPath(fref: &FileRef) -> PathBuf {
        match fref {
            FileRef::StaticFile(pb) => pb.clone(),
            FileRef::TempFile(tf) => tf.path().into(),
        }
    }

    /// Gets the list of Paths (one or more) associated with a DesignatedFile or
    /// returns an error if there is no Path.  The `to_path` method should be
    /// used if only a single path is expected, and this method should be used
    /// when there is a valid potential for multiple paths to exist. The `cmd`
    /// and `path_of` arguments are informative for error generation.
    pub fn to_paths(&self, op_label: &str, to_what: &str) -> anyhow::Result<Vec<PathBuf>>
    {
        match self {
            DesignatedFile::SingleFile(fref) =>
                Ok(vec![Self::getPath(&fref.clone())]),
            DesignatedFile::MultiFile(pbs) =>
                Ok(pbs.iter().map(Self::getPath).collect()),
            DesignatedFile::NoDesignatedFile =>
                Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(op_label.into(), to_what.into()))),
        }
    }

}

#[derive(thiserror::Error,Debug)]
pub enum SubProcError {

    #[error("Sub-process {1:} file not specified for operation {0:?}")]
    ErrorMissingFile(String, String),

    #[error("Error {2:?} running command {0:?} {1:?} in dir {3:?}\n{4:}")]
    ErrorRunningCmd(String, Vec<OsString>, Option<i32>, PathBuf, String),

    #[error("Error {2:?} setting up running command {0:?} {1:?} in dir {3:?}")]
    ErrorCmdSetup(String, Vec<OsString>, std::io::Error, PathBuf),

    #[error("Unsupported file for command {0:?}: {1:?}")]
    ErrorUnsupportedFile(String, NamedFile),

    #[error("Multiple inputs files not supported for sub-process command {0:?}")]
    ErrorUnsupportedDesignatedFile(String),
}


impl SubProcOperation {

    pub fn for_(executing : &Executable) -> SubProcOperation  // KWQ: better name
    {
        SubProcOperation {
            exec : executing.clone(),
            args : executing.base_args.iter().map(|x| x.into()).collect(),
            files : FileTransformation::new(),
            // KWQ: inp_file and out_file are resolved later; if they doesn't match
            // // `exec` then SubProcOperation will fail
        }
    }


    /// Changes the name of the command to execute.
    #[inline]
    pub fn set_executable<T>(&mut self, exe: T) -> &mut SubProcOperation  // KWQ: set_cmd?
    where T: Into<PathBuf>
    {
        // self.old_cmd = Operation::Execute(exe.into());
        self.exec = self.exec.set_exe(exe);
        self
    }



    /// Prepares the final/actual argument list that is to be presented to the
    /// command, including lookup and preparation of files that are referenced by
    /// the command.  This function is normally only used internally by the
    /// execute() operation, but it is exposed for testing purposes.
    pub fn finalize_args(&self) -> anyhow::Result<(Vec<OsString>,
                                                   (DesignatedFile, DesignatedFile))>
    {
        let mut args = self.args.clone();
        let files = self.cmd_file_setup(&mut args)?;
        Ok((args, files))
    }

    // Sets up file references for running a command
    fn cmd_file_setup(&self, args: &mut Vec<OsString>)
                      -> anyhow::Result<(DesignatedFile, DesignatedFile)>
    {
        let inpfiles;
        let outfile;
        // Note: order of file specification is important below because
        // setup_file has side-effects of modifying the args.   KWQ!
        if self.emit_output_file_first() {
            outfile = self.setup_exe_file(
                args, &self.exec.out_file, &self.files.out_filename,
                || Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                   String::from("output (first)")))))?;
            inpfiles = self.setup_exe_file(
                args, &self.exec.inp_file, &self.files.inp_filename,
                || Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                   String::from("input (append)")))))?;
        } else {
            inpfiles = self.setup_exe_file(
                args, &self.exec.inp_file, &self.files.inp_filename,
                || Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                   String::from("input (first)")))))?;
            outfile = self.setup_exe_file(
                args, &self.exec.out_file, &self.files.out_filename,
                || Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                   String::from("output (append)")))))?;
        }
        Ok((inpfiles, outfile))
    }

    // Output option arguments before positional arguments because some command's
    // parsers are limited in this way.  This function returns true if the output
    // file should be specified before the input file; the normal order is input
    // file and then output file (e.g. "cp inpfile outfile").
    fn emit_output_file_first(&self) -> bool
    {
        if let ExeFileSpec::Option(_) = self.exec.out_file {
            if let ExeFileSpec::Append = self.exec.inp_file {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Resolves a FileSpec and insert the actual named file into the argument
    /// list.  This also returns the file; the file may be a temporary file
    /// object which will delete the file at the end of its lifetime, so the
    /// returned value should be held until the file is no longer needed.
    fn setup_file_args(&self,
                       args: &mut Vec<OsString>,
                       spec: &ExeFileSpec,
                       for_file: &DesignatedFile)
                      -> anyhow::Result<()>
    {
        match spec {
            ExeFileSpec::NoFileUsed => Ok(()),
            ExeFileSpec::Append => {
               let pths = for_file.to_paths("subprocop", "exe_file")?; // KWQ: better args
                for pth in pths {
                    args.push(OsString::from(pth.clone().into_os_string()));
                }
                Ok(())
            }
            ExeFileSpec::Option(optflag) => {
                args.push(OsString::from(optflag));
                let pths = for_file.to_paths("subprocop", "exe_file")?;
                let fnames = pths.iter()
                    .map(|x| x.to_str().unwrap()).collect::<Vec<_>>();
                args.push(OsString::from(fnames.join(",")));
                Ok(())
            }
            ExeFileSpec::ViaCall(userfun) => { userfun(args, for_file) }
        }
    }

    /// Resolves a FileSpec and insert the actual named file into the argument
    /// list.  This also returns the file; the file may be a temporary file
    /// object which will delete the file at the end of its lifetime, so the
    /// returned value should be held until the file is no longer needed.
    fn setup_exe_file<E>(&self,    // KWQ KWQ remove
                         args: &mut Vec<OsString>,
                         spec: &ExeFileSpec,
                         candidate: &NamedFile,
                         on_missing: E)
                         -> anyhow::Result<DesignatedFile>
    where E: Fn() -> anyhow::Result<DesignatedFile> {
        let sf = setup_file(candidate, on_missing)?;
        match spec {
            ExeFileSpec::NoFileUsed => Ok(DesignatedFile::NoDesignatedFile), // KWQ could skip setup_file here...
            ExeFileSpec::Append => {
                let pths = sf.to_paths("subprocop", "exe_file")?; // KWQ better args
                for pth in pths {
                    args.push(OsString::from(pth.clone().into_os_string()));
                }
                Ok(sf)
            }
            ExeFileSpec::Option(optflag) => {
                args.push(OsString::from(optflag));
                let pths = sf.to_paths("subprocop", "exe_file")?;
                let fnames = pths.iter()
                    .map(|x| x.to_str().unwrap()).collect::<Vec<_>>();
                args.push(OsString::from(fnames.join(",")));
                Ok(sf)
            }
            ExeFileSpec::ViaCall(userfun) => { userfun(args, &sf)?; Ok(sf) }  // takes designatedfile instead of namedfile...
        }
    }


    // After the files are setup, this performs the actual run.  See the
    // documentation for execute() above for a description of the handling of the
    // cwd parameter.  If echo is true, the command is echoed to stdout just
    // before execution.
    fn run_cmd(&self, cwd: &Path,
               inpfiles : DesignatedFile,
               outfile : DesignatedFile,
               args : Vec<OsString>,
               echo : bool)
               -> anyhow::Result<DesignatedFile>
    {
        let fromdir = self.files.in_dir.clone().unwrap_or_else(|| cwd.to_path_buf());
        if echo {
            println!("{:?} {:?} [in {:?}]",
                     self.exec,
                     args.iter().map(|x| x.to_str().unwrap())
                     .collect::<Vec<_>>().join(" "),
                     fromdir);
        }
        match process::Command::new(&self.exec.exe_file)
            .args(&args)
            .current_dir(&fromdir)
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::piped())
            .spawn()
        {
            Ok(child) => {
                let out = child.wait_with_output()?;
                if !out.status.success() {
                    return Err(anyhow::Error::new(
                        SubProcError::ErrorRunningCmd(
                            format!("{:?}", self.exec), args,
                            out.status.code(),
                            fromdir.to_path_buf(),
                            String::from_utf8_lossy(&out.stderr).into_owned())))
                }
            }
            Err(e) => {
                return Err(anyhow::Error::new(
                    SubProcError::ErrorCmdSetup(format!("{:?}", self.exec),
                                                args, e,
                                                fromdir.to_path_buf())))
            }
        }
        Ok(outfile)
    }
}

impl FilesPrep for SubProcOperation {
    fn set_dir<T>(&mut self, tgtdir: T) -> &mut Self
    where T: AsRef<Path>
    {
        self.files.set_dir(tgtdir);
        self
    }

    fn set_input_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.files.set_input_file(fname);
        self
    }

    fn set_output_file(&mut self, fname: &NamedFile) -> &mut Self
    {
        self.files.set_output_file(fname);
        self
    }
}

impl OpInterface for SubProcOperation {

    fn label(&self) -> &str { self.exec.exe_file.to_str().unwrap_or("{an-exe}") }


    #[inline]
    fn push_arg<T>(&mut self, arg: T) -> &mut SubProcOperation
    where T: Into<OsString>
    {
        self.args.push(arg.into());
        self
    }

    fn execute(&self, cwd: &Path, echo : bool) -> anyhow::Result<DesignatedFile>
    {
        let (args, (inpfiles, outfile)) = self.finalize_args()?;
        self.run_cmd(cwd, inpfiles, outfile, args, echo)
    }

    fn execute_with_inp_override(&self,
                                 cwd: &Path,
                                 inps: &Vec<PathBuf>,
                                 echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        if inps.len() == 0 {
            return self.execute(cwd, echo);
        }

        let mut args = self.args.clone();
        let mut outfile = DesignatedFile::NoDesignatedFile;
        if self.emit_output_file_first() {
            outfile = self.setup_exe_file(
                &mut args, &self.exec.out_file, &self.files.out_filename,
                || Err(anyhow::Error::new(
                    SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                   String::from("output/1")))))?;
        }

        let inpfiles = setup_files(inps, &None)?;
        self.setup_file_args(&mut args, &self.exec.inp_file, &inpfiles)?;

        if !self.emit_output_file_first() {
            outfile = self.setup_exe_file(
                &mut args, &self.exec.out_file, &self.files.out_filename,
                // It is OK here to not have an output file (e.g. tar xvf
                // foo.tar) although if used as part of a ChainedSubOps sequence,
                // the following element in the sequence should have some other
                // method of determining the input file than expecting it to be
                // the propagation of this Op's output file.
                || Ok(DesignatedFile::NoDesignatedFile))?;
                // || Err(anyhow::Error::new(
                //     SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                //                                    String::from("output/2")))))?;
        }
        self.run_cmd(cwd, inpfiles, outfile, args, echo)
    }


    fn execute_with_file_overrides(&self,
                                   cwd: &Path,
                                   inps: &Vec<PathBuf>,
                                   out: &Option<PathBuf>,
                                   echo : bool
    ) -> anyhow::Result<DesignatedFile> {
        match &out {
            None => self.execute_with_inp_override(cwd, inps, echo),
            Some (outf) => {
                let mut args = self.args.clone();
                let outfile_first =
                    if self.emit_output_file_first() {
                        Some(self.setup_exe_file(
                            &mut args,
                            &self.exec.out_file,
                            &NamedFile::Actual(outf.clone()),
                            || Err(anyhow::Error::new(
                                SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                               String::from("output/3")))))?)
                    } else { None };
                let inpfiles = setup_files(inps,
                                           &Some(self.files.inp_filename.clone()))?;
                self.setup_file_args(&mut args, &self.exec.inp_file, &inpfiles)?;
                let outfile = match outfile_first {
                    Some(f) => f,
                    None => {
                        self.setup_exe_file(
                        &mut args,
                        &self.exec.out_file,
                        &NamedFile::Actual(outf.clone()),
                        || Err(anyhow::Error::new(
                            SubProcError::ErrorMissingFile(format!("{:?}", self.exec),
                                                           String::from("output/4")))))?
                    }
                };
                self.run_cmd(cwd, inpfiles, outfile, args, echo)
            }
        }
    }
}

fn replace(pat : &String, subs : &OsString, inpstr : &OsString) -> OsString
{
    match subs.clone().into_string() {
        Ok(sub) => match inpstr.clone().into_string() {
            Ok(inps) => OsString::from(inps.replace(pat, &sub)),
            Err(orig) => orig
        }
        Err(_) => inpstr.clone()

    }
}

// ----------------------------------------------------------------------
/// Chained sub-process operations
///
/// General notes about structure organization:
///
///   The ChainedSubProcOperations is the core structure that contains the list
///   of operations that should be chained together, along with the initial input
///   file and final output file.
///
///   When adding an operation to ChainedSubProcOperations (via .push_op()) the
///   return value should allow subsequent examination/manipulation of that
///   specific operation in the chain (the ChainedOpRef struct).  To do so, and
///   honor Rust's ownership rules, this means that the result references the
///   core ChainedSubProcOperations via a reference counted (Rc) cell (RefCell)
///   to maintain a single copy via the Rc but allow updates of that object via
///   the RefCell.
///
///   To hide the complexity of the Rc<RefCell<ChainedSubProcOperations>> from
///   the user, this value is wrapped in the ChainedSubOps struct.
///
///   User API operations are therefore primarily defined for the ChainedSubOps
///   and ChainedOpRef structs.
///
///   The typical API usage:
///
///    let all_ops = ChainedSubOps::new()
///    let op1 = all_ops.push_op(
///               SubProcOperation::new("command",
///                                     <how to specify input file to command>,
///                                     <how to specify output file to command>))
///    let op2 = all_ops.push_op(
///               SubProcOperation::new("next-command",
///                                     <how to specify input file>,
///                                     <how to specify output file>))
///    ...
///    op1.push_arg("-x")
///    op2.push_arg("-f")
///    op2.push_arg(filename)
///    op2.disable()
///    ...
///    all_ops.set_input_file_for_chain(input_filename)
///    all_ops.set_output_file_for_chain(output_filename)
///    match all_ops.execute() {
///      Err(e) => ...,
///      Ok(sts) -> ...,
///    }

/// Internal structure managing the chain of operations
#[derive(Debug)]
struct ChainedSubProcOperations {    // KWQ --> ChainedIntOps
    chain : Vec<RunnableOp>,
    initial_inp_file : Option<PathBuf>,  // KWQ use FileTransformation
    final_out_file : Option<PathBuf>,
    disabled : Vec<usize>
}

/// This is returned when a RunnableOp is added to the ChainedOps/ChainedIntOps
/// and serves as a proxy for the RunnableOp as it exists in the chain.  This
/// supports additional customization actions on the contained RunnableOp via the
/// FilesPrep trait.
#[derive(Clone,Debug)]
pub struct ChainedOpRef {
    opidx : usize,
    chop : Rc<RefCell<ChainedSubProcOperations>>  // cloned from ChainedSubOps.chops
}


pub struct ChainedSubOps {    // KWQ --> ChainedOps
    chops : Rc<RefCell<ChainedSubProcOperations>>
}

impl fmt::Debug for ChainedSubOps {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.chops.borrow(), f)
    }
}

impl ChainedSubOps {
    // The result is Rc'd so that the ChainedOpRef instances can have a
    // reference to the target as well.
    pub fn new() -> ChainedSubOps
    {
        ChainedSubOps {
            chops :
            Rc::new(
                RefCell::new(
                    ChainedSubProcOperations { chain : Vec::new(),
                                               initial_inp_file : None,
                                               final_out_file : None,
                                               disabled : Vec::new()
                    }
                )
            )
        }
    }
}


impl ChainedSubOps   // KWQ some of these should just be impl OpInterface
{
    /// Adds a new SubProcOperation operation to the end of the chain.  Returns a
    /// reference for modifying that operation.
    pub fn push_op(self: &ChainedSubOps, op: &SubProcOperation) -> ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chops.borrow_mut();
            ops.chain.push(RunnableOp::Exec(op.clone()));
        }
        ChainedOpRef { opidx : self.chops.borrow().chain.len() - 1,
                       chop : Rc::clone(&self.chops)
        }
    }

    /// Adds a new FunctionOperation operation to the end of the chain.  Returns
    /// a reference for modifying that operation.
    pub fn push_call(self: &ChainedSubOps, op: &FunctionOperation) -> ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chops.borrow_mut();
            ops.chain.push(RunnableOp::Call(op.clone()));
        }
        ChainedOpRef { opidx : self.chops.borrow().chain.len() - 1,
                       chop : Rc::clone(&self.chops)
        }
    }

    /// Sets the input file(s) for the entire chain
    #[inline]
    pub fn set_inp_file_for_chain(&self, inp_file: &Option<PathBuf>) -> ()  // KWQ: spell out input and output here and elsewhere?
    {
        let mut ops: RefMut<_> = self.chops.borrow_mut();
        ops.initial_inp_file = inp_file.clone();  // KWQ: append!
    }

    /// Retrieves the name of input file providing the original input to the
    /// entire chain.
    #[inline]
    pub fn inp_file_for_chain(&self, inp_file: &Option<PathBuf>) -> Option<PathBuf>
    {
        self.chops.borrow().initial_inp_file.clone()
    }

    /// Sets the output file for the entire chain (i.e. the end file)
    #[inline]
    pub fn set_out_file_for_chain(&self, out_file: &Option<PathBuf>) -> ()
    {
        let mut ops: RefMut<_> = self.chops.borrow_mut();
        ops.final_out_file = out_file.clone();
    }

    /// Gets the output file path for the end of the chain.  Returns None if the
    /// output file is not specified or is indefinite/temporary and therefore
    /// cannot be accessed.
    #[inline]
    pub fn out_file_for_chain(&self) -> Option<PathBuf>
    {
        self.chops.borrow().final_out_file.clone()
    }

    /// Executes all the enabled operations in this chain sequentially, updating
    /// the input file of each operation to be the output file from the previous
    /// operation.  On success, returns the number of operations executed.
    ///
    /// The directory parameter specifies the default directory from which the
    /// chained operations will be performed.  Each chained operation might
    /// operate from a separate directory if the SubProcOperation::set_dir() or
    /// ChainedOpRef::set_dir() function has been called for this operation,
    /// which overrides the default directory passed to this command.
    pub fn execute<T>(&self, cwd: &Option<T>, echo : bool) -> anyhow::Result<usize>
    where T: Into<PathBuf>, T: Clone
    {
        let curdir = match &cwd {
            Some(p) => p.clone().into(),
            None => current_dir()?
        };
        let chops = self.chops.borrow();
        // n.b. cannot Clone the chain (thus, cannot alter it), so instead build
        // a vec of the valid indices.  Build it in reverse so the operations can
        // simply .pop() the next index off the end.
        let mut enabled_opidxs : Vec<usize> = chops.chain.iter()
            .enumerate()
            .filter(|(i,_op)| ! chops.disabled.contains(i))
            .map(|(i,_op)| i)
            .rev()
            .collect();
        execute_chain(&chops.chain, curdir.as_path(), &mut enabled_opidxs,
                      &match &chops.initial_inp_file {
                          Some(f) => vec![f.clone()],
                          None => vec![]
                      },
                      &chops.final_out_file,
                      echo)
    }
}

fn execute_chain(chops: &Vec<RunnableOp>,
                 cwd: &Path,
                 mut op_idxs: &mut Vec<usize>,
                 inp_files : &Vec<PathBuf>,  // usually just one, except GlobIn
                 out_file : &Option<PathBuf>,
                 echo : bool)
                 -> anyhow::Result<usize>
{
    let op_idx = op_idxs.pop().unwrap();
    let spo = &chops[op_idx];
    let last_op = op_idxs.is_empty();
    if last_op {
        spo.execute_with_file_overrides(cwd, inp_files, out_file, echo)?;
        return Ok(1);
    }

    let outfile = spo.execute_with_inp_override(cwd, inp_files, echo)?;
    let nxt_inp_or_err =  outfile.to_paths(spo.label(), "input_file(s)");
    let nxt_inpfile = match nxt_inp_or_err {
        Ok(_) => nxt_inp_or_err,
        Err(ref e) => match e.root_cause().downcast_ref::<SubProcError>() {
            Some(SubProcError::ErrorMissingFile(_, _)) =>
                // This is OK here because the following operation may be setup
                // for not needing an input file specification; if it does need
                // one then presumably some subsequent runtime check will
                // validate its (lack of) existence and signal a useful failure.
                Ok(Vec ::new()),
            _ => nxt_inp_or_err,
        },
    }?;
    let nxt = execute_chain(chops, cwd, &mut op_idxs, &nxt_inpfile, &out_file, echo)?;
    Ok(nxt + 1)
}

impl ChainedOpRef {   // KWQ FilesPrep trait


    /// Add an argument to this operation in the chain
    #[inline]
    pub fn push_arg<T>(&self, arg: T) -> &ChainedOpRef
    where T: Into<OsString>
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.chain[self.opidx].push_arg(arg);
        }
        self
    }

    /// Sets the default directory for execution of this operation
    pub fn set_dir<T>(&self, tgtdir: T) -> &ChainedOpRef
    where T: AsRef<Path>
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.chain[self.opidx].set_dir(tgtdir);
        }
        self
    }

    /// Enables this operation in the chain.  By default, an operation added to
    /// the chain is automatically enabled, but it can be explicitly disabled or
    /// enabled prior to execution.  See disable() for more information.
    #[inline]
    pub fn enable(&self) -> &ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.disabled.retain(|&x| x != self.opidx);
        }
        self
    }

    /// Disables this operation in the chain.  By default, an operation added to
    /// the chain is automatically enabled, but it can be explicitly disabled or
    /// enabled prior to execution.
    /// This is useful for building a chain
    /// consisting of all possible operations and then "removing" those that are
    /// subsequently determined not to be needed by disabling them.
    #[inline]
    pub fn disable(&self) -> &ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.disabled.push(self.opidx);
        }
        self
    }

    /// Sets the input file specification for this operation, overriding any
    /// previous specification.
    pub fn set_input(&self, inp_fname : &NamedFile) -> &ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.chain[self.opidx].set_input_file(inp_fname);
        }
        self
    }

    /// Sets the output file specification for this operation, overriding any
    /// previous specification.
    pub fn set_output(&self, out_fname : &NamedFile) -> &ChainedOpRef
    {
        {
            let mut ops: RefMut<_> = self.chop.borrow_mut();
            ops.chain[self.opidx].set_output_file(out_fname);
        }
        self
    }
}

// ----------------------------------------------------------------------
// TESTS
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_append_append() -> () {
        let exe = Executable::new(&"test-cmd",
                                  ExeFileSpec::Append,
                                  ExeFileSpec::Append);
        let op = SubProcOperation::for_(&exe)
            .set_input_file(&NamedFile::actual("inpfile.txt"))
            .set_output_file(&NamedFile::temp(".out"))
            .push_arg("-a")
            .push_arg("a-arg-value")
            .push_arg("-b")
            .clone();

        let mut args = op.args.clone();
        let setup = op.cmd_file_setup(&mut args);
        assert_eq!(args[..args.len()-1].to_vec(),
                   &["-a", "a-arg-value", "-b", "inpfile.txt"]);
        assert!(match setup {
            Ok((DesignatedFile::SingleFile(FileRef::StaticFile(inf)),
                DesignatedFile::SingleFile(FileRef::TempFile(_)))) =>
                inf == PathBuf::from("inpfile.txt"),
// true, // (inps.len() == 1)
            // && inps[0] = DesignatedFile::StaticOutputFile("inpfile.txt".into())
// p                KWQ ,

            _ => false
        });
    }

    #[test]
    fn test_append_option() -> () {
        let exe = Executable::new(&"test-cmd",
                                  ExeFileSpec::Append,
                                  ExeFileSpec::Option("-o".into()));
        let op = SubProcOperation::for_(&exe)
            .set_input_file(&NamedFile::actual("inpfile.txt"))
            .set_output_file(&NamedFile::actual("outfile.out"))
            .push_arg("-a")
            .push_arg("a-arg-value")
            .push_arg("-b")
            .clone();

        let mut args = op.args.clone();
        let outfile = op.cmd_file_setup(&mut args);
        assert_eq!(args,
                   &["-a", "a-arg-value", "-b", "-o", "outfile.out", "inpfile.txt"]);
        assert!(match outfile {
            Ok((_, DesignatedFile::SingleFile(FileRef::StaticFile(p)))) =>
                p == PathBuf::from("outfile.out"),
            _ => false
        });
    }

    #[derive(Debug)]
    struct Called(PathBuf, DesignatedFile, DesignatedFile);

    // If the string contains "<pfx>TEMPFILE#nnn.<sfx>", returns Some(pfxlen, nnn
    // : usize, sfxlen.  If the string does not contain TEMPFILE#, returns None.
    // Only the first TEMPFILE# is detected.
    fn extract_tempfileref(arg: &OsString) -> Option<(usize, usize, usize)>
    {
        let argstr = arg.to_str()?;
        match argstr.find("TEMPFILE#") {
            None => None,
            Some(si) => {
                let ni = si + "TEMPFILE#".len();
                let (np, sp) = argstr[ni..].split_once('.')?;
                let nn : usize = np.parse().ok()?;
                Some((si, nn, sp.len()))
            }
        }
    }

    // Extracts the temp file reference from arg and checks it against the record
    // of that indexed temp file (or sets the index storage if not seen before).
    // Ensures that arg matches the temp file and the remainder of arg matches
    // against (which contained the original tempfileref).
    fn match_tempref(temps : &mut Vec<OsString>,
                     tempfileref : (usize, usize, usize),
                     arg_os : &OsString,
                     against : &OsString)
                     -> bool
    {
        let (pl, tn, sl) = tempfileref;
        let arg = match arg_os.to_str() {
            Some(s) => s,
            None => return false,
        };
        let al = arg.len();
        if al <= pl + sl { return false; }
        let (op, rp) = arg.split_at(pl);
        let (tf, os) = if sl == 0 { (rp, "") } else { rp.split_at(al - sl) };
        match temps.get(tn) {
            None => {
                temps.resize(tn + 1, "".into());
                temps[tn] = tf.into();
            },
            Some(f) =>
                if f == "" {
                    temps[tn] = tf.into();
                } else if f != tf {
                    return false;
                }
        }
        let o = match against.to_str() {
            Some(s) => s,
            None => return false,
        };
        let r = o.split_at(pl).0 == op &&
            (if sl == 0 { "" } else { o.split_at(o.len()-sl).1 }) == os;
        return r;
    }

    // Performs equality checking between two parallel Called objects, allowing
    // for substitutions of temporary file references in the arguments with a
    // consistent
    fn eq_with_temps(tempfiles : &mut Vec<OsString>, a: &Called, b: &Called)
                     -> bool
    {
        let Called(p1, i1, o1) = a;
        let Called(p2, i2, o2) = b;
        if p1 != p2 { return false; }
        true // KWQ: need to check o1/o2 and i1/i2
        // i1 == i2 && o1 == o2  // KWQ: need to match against temp fies

        // for arg in v1.into_iter().enumerate() {
        //     match v2.get(arg.0) {
        //         None => return false,  // argument count mismatch
        //         Some(o) => {
        //             match extract_tempfileref(arg.1) {
        //                 None =>
        //                     match extract_tempfileref(o) {
        //                         None => if arg.1 != o { return false },
        //                         Some(m) =>
        //                             if !match_tempref(tempfiles, m, arg.1, o) {
        //                                 return false;
        //                             }
        //                     }
        //                 Some(m) =>
        //                     if !match_tempref(tempfiles, m, o, arg.1) {
        //                         return false;
        //                     }
        //             }
        //         } // argument value mismatch
        //     }
        // }
        // true
    }

    impl PartialEq for Called {
        fn eq(&self, other: &Self) -> bool {
            let mut tempfiles : Vec<OsString> = vec![];
            eq_with_temps(&mut tempfiles, self, other)
        }
    }

    // Compares two vectors of Called objects, with persisted temporary name
    // substitutions.
    fn compare_called_vecs(actual : &Vec<Called>, expected : &[Called])
                           -> anyhow::Result<()>
    {
        println!("{} CALLS", actual.len());
        for call in actual {
            println!("  * {:?}", call);
        }
        assert_eq!(actual.len(), expected.len());
        let mut tempfiles : Vec<OsString> = vec![];
        for idx in 0..actual.len() {
            if !eq_with_temps(&mut tempfiles, &actual[idx], &expected[idx]) {
                assert_eq!(actual[idx], expected[idx]);
                assert!(false);  // just in case
            }
        }
        Ok(())
    }

//    #[test]
    fn test_chain() -> anyhow::Result<()> {
        let ops = ChainedSubOps::new();
        // ops.set_inp_file_for_chain(&Some("orig.inp".into()));
        ops.set_out_file_for_chain(&Some("final.out".into()));

        let exec : Rc<RefCell<Vec<Called>>> = Rc::new(RefCell::new(vec![]));
        let erec = exec.clone();
        let record_exec = move |cwd : &Path, inps : &DesignatedFile, outs : &DesignatedFile| Ok(
            erec.borrow_mut().push(Called(cwd.to_path_buf(),
                                          DesignatedFile::NoDesignatedFile, // KWQ: inps,
                                          DesignatedFile::NoDesignatedFile // KWQ: outs
            )));

        let rslt = {
            // let exe1 = Executable::new(&"test-cmd", ExeFileSpec::Append, ExeFileSpec::Option("-o".into()));   KWQ
            let op1 = ops.push_call(&FunctionOperation::calling("op1",
                                                                record_exec.clone()));
            op1.set_input(&NamedFile::TBD);
            op1.set_output(&NamedFile::temp(".c"));
            op1.push_arg("--medium");

            let op2 = ops.push_call(&FunctionOperation::calling("op2",
                                                                record_exec.clone()));
            op2.set_input(&NamedFile::TBD);
            op2.set_dir("/tmp");
            op2.push_arg("-s");
            op2.push_arg("direct");
            op2.push_arg("--style=call");
            op2.set_output(&NamedFile::TBD);

            let op3 = ops.push_call(&FunctionOperation::calling("op3",
                                                                record_exec.clone()));
            op3.set_input(&NamedFile::temp(".wow"));
            op3.set_output(&NamedFile::temp(".zap"));
            op3.push_arg("--crazy");
            op3.disable();

            let op4 = ops.push_call(&FunctionOperation::calling("op4",
                                                                record_exec.clone()));
            // n.b. expects cargo-test run from top-level, where there are two
            // files that will match this glob.  This test could be improved by
            // creating a tempdir with specific files populating that tempdir...
            op4.set_input(&NamedFile::glob_in(".", "LICENSE-*"));
            op4.set_output(&NamedFile::TBD);
            op4.push_arg("--opnum=4");

            let op5 = ops.push_call(&FunctionOperation::calling("op5",
                                                                record_exec.clone()));
            op5.set_input(&NamedFile::glob_in(".", "LICENSE-*"));
            op5.set_output(&NamedFile::temp(".lic"));
            op5.push_arg("--opnum=5");
            op5.push_arg("--inputs={LICENSES}");
            op5.push_arg("-c");
            op5.disable();

            let op6 = ops.push_call(&FunctionOperation::calling("op6",
                                                                record_exec.clone()));
            op6.set_input(&NamedFile::TBD);
            op6.set_output(&NamedFile::TBD);
            op6.push_arg("--copy-from={INP}");

            op5.enable();

            ops.execute::<String>(&None, true)
        };

        assert_eq!(5, rslt?);
        let here : PathBuf = current_dir()?.into();
        compare_called_vecs(&*exec.borrow(),
                            &[Called(here.clone(),
                                     DesignatedFile::NoDesignatedFile,
                                     DesignatedFile::NoDesignatedFile),
                            // ["--medium",
                            //  "-o",
                            //  "TEMPFILE#0.",
                            // ].map(|x| x.into()).to_vec()),
                     Called("/tmp".into(),
                                     DesignatedFile::NoDesignatedFile,
                                     DesignatedFile::NoDesignatedFile),
                            // ["-s",
                            //  "direct",
                            //  "--style=call",
                            //  "TEMPFILE#0.",
                            // ].map(|x| x.into()).to_vec()),
                     Called(here.clone(),
                                     DesignatedFile::NoDesignatedFile,
                                     DesignatedFile::NoDesignatedFile),
                            // ["--opnum=4",
                            //  "LICENSE-APACHE",
                            //  "LICENSE-MIT",
                            // ].map(|x| x.into()).to_vec()),
                     Called(here.clone(),
                                     DesignatedFile::NoDesignatedFile,
                                     DesignatedFile::NoDesignatedFile),
                            // ["--opnum=5",
                            //  "--inputs=LICENSE-APACHE,LICENSE-MIT",
                            //  "-c",
                            //  "TEMPFILE#1.",
                            // ].map(|x| x.into()).to_vec()),
                     Called(here.clone(),
                                     DesignatedFile::NoDesignatedFile,
                                     DesignatedFile::NoDesignatedFile),
                            // ["--copy-from=TEMPFILE#1.",
                            //  "final.out",
                            // ].map(|x| x.into()).to_vec()),
                   ])?;

        Ok(())
    }

}

// Executable { exe_file:PathBuf, base_args:Vec<String>, inp_file:ExeFileSpec, out_file:ExeFileSpec }
//    .new, .push_arg(a), .set_exe(e)
// ExeFileSpec { NoFileUsed, Append, Option(String), ViaCall(fn) }
// NamedFile { Temp(String), Actual(PathBuf), GlobIn(PathBuf, String), TBD }
// FileSpec { Unneeded, Append(NamedFile), Option(String, NamedFile), Replace(String, NamedFile)

// Issue is that FileSpec encodes both *what* and *how*, mixing the two.  Newly, the Executable specifies *how*, so SubProcOperation should only need *what*, which is NamedFile.

// SubProcOperation { exec:Executable, cmd:Operation, args:Vec<OsString>, inp_file:FileSpec, out_file:FileSpec, in_dir:Option<PathBuf>, inp_filename:NamedFile, out_filename:NamedFile }
//    .new, .for_, .calling, .set_executable, .push_arg, .set_input_file, .set_output_file, --> .output, .set_dir, .execute, .execute_with_inp_override, .execute_with_file_overrides
// Operation { Execute(OsString), Call(fn) }
// ChainedSubProcOperations { chain:Vec<SubProcOperation>, initial_inp_file:Option<PathBuf>, final_out_file:Option<PathBuf>, disabled:Vec<usize> }
//    .push_op -> ChainedOpRef, .inp_file_for_chain, .set_out_file_for_chain, .out_file_for_chain, .execute
// execute_chain
// ChainedOpRef
//    .push_arg, .set_dir, .enable, .disable, .set_input, .set_output

// DesignatedFile { NoDesignatedFile, StaticOutputFile(PathBuf), StaticOutputFiles(Vec<PathBuf>), TempOutputFile(tempfile::NamedTempFile) }
