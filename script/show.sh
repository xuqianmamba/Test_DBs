#for system calls
echo "read() calls:" && grep -c "read(" strace_output.txt
echo "write() calls:" && grep -c "write(" strace_output.txt
echo "open() calls:" && grep -c "open(" strace_output.txt
echo "close() calls:" && grep -c "close(" strace_output.txt
