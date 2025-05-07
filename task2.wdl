version 1.0

workflow ProcessAssemblyParallel {
    input {
        File assembly_file
    }

    call SplitSequences {
        input:
            assembly_file = assembly_file
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

    command <<<
        apt-get update && apt-get install -y samtools && rm -rf /var/lib/apt/lists/* && \
        gzip -d -c ~{assembly_file} > unzipped.fa && \
        assembly=unzipped.fa && \
        samtools faidx "$assembly" && \
        cut -f1 ~{assembly_file}.fai | while read -r seq; do \
            samtools faidx "$assembly" "$seq" > "$seq.fasta" \
        done && \
        ls *.fasta > split_sequences.txt
    >>>

    output {
        Array[File] split_output = read_lines("split_sequences.txt")
    }

    runtime {
        docker: "ubuntu:latest"
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
         grep -v "^>" ~{assembly_sequence} | grep -o -i 'N' | tr -d "\n" | wc -m > gap_length.txt
    }

    output {
        Int gap_length = read_int("gap_length.txt")
    }

    runtime {
        docker: "ubuntu:latest"
        memory: "4 GB"
        cpu: 1
        preemptible: 2
    }
}

task MergeResults {
    input {
        Array[Int] gap_counts
    }

    command <<<
    echo "~{sep=" + " gap_counts}" | bc > total_gap_length.txt
    >>>

    output {
        Int total_gap_length = read_int("total_gap_length.txt")
    }

    runtime {
        docker: "ubuntu:latest"
        memory: "4 GB"
        cpu: 1
        preemptible: 2
    }
}
