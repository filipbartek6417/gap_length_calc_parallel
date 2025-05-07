version 1.0

workflow ProcessAssemblyParallel {
    input {
        File assembly_file
    }

    scatter (sequence in SplitSequences.split_output) {
        call CountGaps {
            input:
                assembly_sequence = sequence
        }
    }

    call MergeResults {
        input:
            gap_counts = CountGaps.gap_length
    }

    output {
        Int total_gap_length = MergeResults.total_gap_length
    }
}

task SplitSequences {
    input {
        File assembly_file
    }

    command {
        awk '/^>/ { if (seq) close(seq); seq = substr($1, 2) ".fasta"; print > seq; next } { print >> seq }' ~{assembly_file}
        echo $(ls *.fasta | tr '\n' ',') > split_sequences.txt
    }

    output {
        Array[File] split_output = read_lines("split_sequences.txt")
    }

    runtime {
        docker: "debian:bullseye"
        memory: "4 GB"
        cpu: 1
        preemptible: 2
    }
}

task CountGaps {
    input {
        File assembly_sequence
    }

    command {
        grep -o "[Nn\\-]+" ~{assembly_sequence} | awk '{ total += length($0) } END { print total }' > gap_length.txt
    }

    output {
        Int gap_length = read_int("gap_length.txt")
    }

    runtime {
        docker: "debian:bullseye"
        memory: "4 GB"
        cpu: 1
        preemptible: 2
    }
}

task MergeResults {
    input {
        Array[Int] gap_counts
    }

    command <<EOF
    echo "~{sep=" + " gap_counts}" | bc > total_gap_length.txt
    EOF

    output {
        Int total_gap_length = read_int("total_gap_length.txt")
    }

    runtime {
        docker: "debian:bullseye"
        memory: "4 GB"
        cpu: 1
        preemptible: 2
    }
}
