package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Factory to retrieve an {@link InContainer} instance with.
 */
public enum InContainerFactory
{
	/**
	 * Container that adds data to a {@link StringBuilder}.
	 */
	STRING, /**
			 * Container that adds data to a {@link StringBuilder} together with a newline placed right after each added
			 * {@link String}.
			 */
	STRINGWITHLINESEPERATOR
	{
		/**
		 * Retrieve an {@link StringInContainer} instance where a newline is added after each {@code add(}{@link String}
		 * {@code line)}.
		 * 
		 * @return {@link StringInContainer}
		 */
		@Override
		public InContainer getContainer()
		{
			return new StringInContainer()
			{
				/**
				 * Adds the line to the container with an added newline on the end.
				 * 
				 * @param line
				 */
				@Override
				public void add(String line)
				{
					super.add(line + System.lineSeparator());
				}
			};
		}
	};

	/**
	 * Retrieve an {@link StringInContainer} instance.
	 * 
	 * @return {@link StringInContainer}
	 */
	public InContainer getContainer()
	{
		return new StringInContainer();
	}
}
